# Transactional outbox demo

A small, runnable demonstration of the
[Transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html)
pattern.

A service often has to update its database **and** tell the rest of the world
about it. Doing both reliably is the problem: a message sent inside a database
transaction can be delivered even if the transaction rolls back, and a message
sent after the commit can be lost if the process crashes first. A distributed
transaction (2PC) across the database and the broker is usually unavailable or
undesirable.

The outbox pattern removes the problem by writing the event into the **same
local transaction** as the business data, and letting a separate relay publish
it afterwards:

```text
placeOrder(cmd)
   │
   ├─ BEGIN ───────────────────────────────────┐
   │    INSERT INTO orders ...                 │  one local ACID transaction
   │    INSERT INTO outbox (event payload)     │  (no 2PC, no broker call)
   │  COMMIT ──────────────────────────────────┘
   │
   └─ returns OK ──► relay (polling publisher)
                          │  SELECT ... FROM outbox WHERE published_at IS NULL ORDER BY id
                          │  publish to the broker
                          │  UPDATE outbox SET published_at = now()
                          ▼
                     consumer: INSERT shipment
                               INSERT processed_messages(message_id)   -- same transaction
                               (duplicate message_id ⇒ no-op)
```

The event is guaranteed to exist if and only if the business transaction
committed. Delivery is **at-least-once** — the relay can crash after publishing
but before recording that it did — so the consumer is **idempotent**, which
makes the overall effect exactly-once.

## Requirements

- JDK 21+
- Internet access on the first build so Gradle can download dependencies
  (Gradle itself is provided by the included wrapper)

No Docker, broker or database server is required: the demo uses an in-memory H2
database and a tiny HTTP message broker that run inside the JVM.

## Run

```bash
./gradlew run
```

The demo walks through six steps and prints the table counts after each one so
the invariants are visible:

1. An order and its `OrderPlaced` event are committed together.
2. The relay publishes the pending event; the consumer schedules a shipment.
3. The broker is unavailable: the event stays pending in the outbox and is
   published after the broker recovers — nothing is lost.
4. The relay crashes after publishing but before marking the row: after restart
   the message is delivered twice, and the consumer ignores the duplicate.
5. The business transaction rolls back: there is no order **and** no event.
6. A summary that checks there are no orders without events and no events
   without orders.

Verified output (IDs are random UUIDs and differ on every run):

```text
Transactional outbox demo (H2 + local HTTP broker)
mode: transactional outbox

--- 1. Place an order: the business row and its event commit together ---
BEGIN
  INSERT INTO orders  (id=c18ea658-ac7f-4f29-ab37-347b82fe4cfd, total=42.50, status=PLACED)
  INSERT INTO outbox  (message_id=7ce81f85-3450-433d-971a-a16b93d1b77b, published_at=NULL)
COMMIT
state: orders=1  outbox pending=1  outbox published=0  shipments=0  duplicates ignored=0

--- 2. The relay publishes the pending event ---
relay: publishing OrderPlaced 7ce81f85-3450-433d-971a-a16b93d1b77b (aggregate=c18ea658-ac7f-4f29-ab37-347b82fe4cfd)
consumer: shipment scheduled for order c18ea658-ac7f-4f29-ab37-347b82fe4cfd (message 7ce81f85-3450-433d-971a-a16b93d1b77b)
relay: broker accepted the message (HTTP 202)
relay: marked 7ce81f85-3450-433d-971a-a16b93d1b77b as published
state: orders=1  outbox pending=0  outbox published=1  shipments=1  duplicates ignored=0

--- 3. Broker unavailable: the event stays in the outbox and is not lost ---
placed order 716d29af-dc7a-4b06-8ccd-8aa4a0a29820
relay: publishing OrderPlaced 4289097b-dfd4-4a26-a121-0f285961aa1c (aggregate=716d29af-dc7a-4b06-8ccd-8aa4a0a29820)
relay: publish failed (broker replied HTTP 503 (broker is unavailable)) -> row stays pending for a later retry
state: orders=2  outbox pending=1  outbox published=1  shipments=1  duplicates ignored=0
broker restored
relay: publishing OrderPlaced 4289097b-dfd4-4a26-a121-0f285961aa1c (aggregate=716d29af-dc7a-4b06-8ccd-8aa4a0a29820)
consumer: shipment scheduled for order 716d29af-dc7a-4b06-8ccd-8aa4a0a29820 (message 4289097b-dfd4-4a26-a121-0f285961aa1c)
relay: broker accepted the message (HTTP 202)
relay: marked 4289097b-dfd4-4a26-a121-0f285961aa1c as published
state: orders=2  outbox pending=0  outbox published=2  shipments=2  duplicates ignored=0

--- 4. Relay crash after publishing, before marking: at-least-once delivery ---
placed order 08d2e557-b177-4454-adcf-ab5e638f2e28
relay: publishing OrderPlaced ee0f6fb7-a985-406f-add7-a37f9f57247e (aggregate=08d2e557-b177-4454-adcf-ab5e638f2e28)
consumer: shipment scheduled for order 08d2e557-b177-4454-adcf-ab5e638f2e28 (message ee0f6fb7-a985-406f-add7-a37f9f57247e)
relay: broker accepted the message (HTTP 202)
relay: simulated crash before marking ee0f6fb7-a985-406f-add7-a37f9f57247e as published
state: orders=3  outbox pending=1  outbox published=2  shipments=3  duplicates ignored=0
relay restarted
relay: publishing OrderPlaced ee0f6fb7-a985-406f-add7-a37f9f57247e (aggregate=08d2e557-b177-4454-adcf-ab5e638f2e28)
consumer: message ee0f6fb7-a985-406f-add7-a37f9f57247e already in processed_messages -> ignored (idempotent)
relay: broker accepted the message (HTTP 202)
relay: marked ee0f6fb7-a985-406f-add7-a37f9f57247e as published
state: orders=3  outbox pending=0  outbox published=3  shipments=3  duplicates ignored=1

--- 5. Business transaction rolls back: no order, and no event ---
BEGIN
  INSERT INTO orders  (id=ab88dbf5-2574-4d23-9fee-cd4b084f1460, total=500.00, status=PLACED)
  INSERT INTO outbox  (message_id=b6cdd4c3-da78-4eb4-9c1e-71e064bad2b9, published_at=NULL)
simulated downstream failure -> ROLLBACK
state: orders=3  outbox pending=0  outbox published=3  shipments=3  duplicates ignored=1

--- Summary ---
  orders committed              : 3
  distinct events published     : 3
  messages accepted by broker   : 4 (including redeliveries)
  shipments scheduled           : 3
  duplicate deliveries ignored  : 1
  orders without an event       : 0
  events without an order       : 0
```

## Test

```bash
./gradlew test
```

The test suite verifies each guarantee:

- the order and its outbox row are committed by the same transaction
- a rolled-back business transaction leaves neither an order nor an event
- the relay publishes in outbox order and marks rows only after a successful
  publish
- rows written by an uncommitted transaction are invisible to the relay
- a broker outage leaves the event pending, and a later retry delivers it
- a duplicate `message_id` is a no-op for the consumer
- a crash after publishing causes a redelivery but no duplicate effect
- the naive approach leaves a "phantom" event behind

## The naive alternative

```bash
./gradlew run --args="--naive"
```

This mode publishes the event *before* the database transaction completes. When
the transaction then rolls back, the broker and the consumer have acted on an
order that does not exist — the inconsistency the outbox pattern prevents:

```text
--- Naive approach: publish first, then run the business transaction ---
naive: publishing OrderPlaced directly to the broker, outside any transaction
consumer: shipment scheduled for order 92127a79-0743-4538-851f-f9b2dd4a88ef (message e84abfc3-8f5a-4d54-9b7d-38db08bb2007)
naive: broker accepted the event (HTTP 202)
naive: database write failed -> ROLLBACK (orders table unchanged)

--- State afterwards ---
orders=0  outbox=0  shipments=1  events published=1
```

## How the code maps to the pattern

| Pattern participant | Class | Responsibility |
| --- | --- | --- |
| Sender | `OrderService` | Writes the order and the outbox row in one transaction |
| Database | `Database` (H2) | Stores business data and the outbox |
| Message outbox | `outbox` table | Pending events, ordered by `id`, with `published_at` |
| Message relay | `OutboxRelay` | Polling publisher: publish, then mark published |
| Message broker | `LocalBroker` | Real HTTP endpoint on `127.0.0.1`, can be "down" |
| Consumer | `ShipmentConsumer` | Deduplicates via `processed_messages` in the same transaction |
| Anti-pattern | `NaiveOrderService` | Publishes outside the transaction (for contrast) |

Schema:

```sql
orders             (id PK, customer_id, total, status, created_at)
outbox             (id IDENTITY PK, message_id UNIQUE, aggregate_type, aggregate_id,
                    event_type, payload, created_at, published_at NULL)
shipments          (order_id PK, status, created_at)
processed_messages (message_id PK, processed_at)
```

## Production notes

This demo deliberately keeps everything in one JVM. In production:

- **Polling publisher vs transaction log tailing.** This demo is a *polling
  publisher*: it queries the outbox on an interval. The alternative is
  *transaction log tailing* (e.g. Debezium reading the database's replication
  log and publishing to Kafka), which avoids polling but couples you to the
  database's log format.
- **Multiple relay instances.** Query with `SELECT ... FOR UPDATE SKIP LOCKED`
  (PostgreSQL) or an equivalent, so instances do not publish the same rows
  concurrently. A single-threaded relay preserves outbox order; ordering per
  aggregate (`aggregate_id`) is what consumers usually need.
- **Batching and retention.** Publish in batches, and delete or archive
  published rows on a schedule so the outbox does not grow without bound.
- **Idempotent consumers are mandatory.** Keep the dedupe record in the same
  transaction as the side effect; a separate "check then write" is a race.
- **At-least-once, not exactly-once.** If the relay crashes between the publish
  and the `UPDATE`, the message is sent again. Design for it rather than
  pretending it cannot happen.
- **The developer can still forget the event.** Enforce the pattern at a
  framework level (e.g. an aggregate/repository abstraction, or Spring
  Modulith's event publication registry) rather than by convention.

## Build a standalone distribution

```bash
./gradlew installDist
./build/install/transactional-outbox-example/bin/transactional-outbox-example
```

## Layout

```text
src/main/java/com/example/outbox/OutboxDemo.java         demo script (and --naive mode)
src/main/java/com/example/outbox/OrderService.java       business transaction: order + outbox row
src/main/java/com/example/outbox/NaiveOrderService.java  the anti-pattern, for contrast
src/main/java/com/example/outbox/OutboxRelay.java        polling publisher (message relay)
src/main/java/com/example/outbox/LocalBroker.java        local HTTP message broker
src/main/java/com/example/outbox/ShipmentConsumer.java   idempotent consumer
src/main/java/com/example/outbox/Database.java           H2 connection + schema + transactions
src/test/java/com/example/outbox/TransactionalOutboxTest.java
```

