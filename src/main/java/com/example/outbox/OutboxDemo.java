package com.example.outbox;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * A runnable demonstration of the transactional outbox pattern
 * (https://microservices.io/patterns/data/transactional-outbox.html).
 *
 * <p>An "order service" writes an order and an {@code OrderPlaced} event in one
 * local database transaction. A polling relay publishes pending events to a
 * local HTTP broker, and a shipment consumer applies them idempotently. The
 * narrative shows the failure modes the pattern addresses.</p>
 *
 * <p>Run {@code ./gradlew run --args="--naive"} to see the naive alternative,
 * which publishes the event before the database transaction commits.</p>
 */
public final class OutboxDemo {

    public static void main(String[] args) throws Exception {
        boolean naive = Arrays.asList(args).contains("--naive");

        say("Transactional outbox demo (H2 + local HTTP broker)");
        say("mode: " + (naive
                ? "naive (publish inside the transaction)"
                : "transactional outbox"));

        try (Database database = Database.start(); LocalBroker broker = LocalBroker.start()) {
            ShipmentConsumer consumer = new ShipmentConsumer(database, OutboxDemo::say);
            broker.subscribe(consumer::onEvent);

            if (naive) {
                runNaive(database, broker, consumer);
            } else {
                runOutbox(database, broker, consumer);
            }
        }
    }

    private static void runOutbox(Database database, LocalBroker broker, ShipmentConsumer consumer)
            throws SQLException, BrokerException {
        OrderService orders = new OrderService(database);
        OutboxRelay relay = new OutboxRelay(database, broker, OutboxDemo::say);

        section("1. Place an order: the business row and its event commit together");
        OrderPlaced first = OrderPlaced.create("cust-1001", new BigDecimal("42.50"));
        say("BEGIN");
        say("  INSERT INTO orders  (id=" + first.orderId() + ", total=" + first.total() + ", status=PLACED)");
        say("  INSERT INTO outbox  (message_id=" + first.messageId() + ", published_at=NULL)");
        orders.placeOrder(first);
        say("COMMIT");
        state(database, consumer);

        section("2. The relay publishes the pending event");
        relay.publishPending();
        state(database, consumer);

        section("3. Broker unavailable: the event stays in the outbox and is not lost");
        OrderPlaced second = orders.placeOrder("cust-1002", new BigDecimal("19.99"));
        say("placed order " + second.orderId());
        broker.setAvailable(false);
        try {
            relay.publishPending();
        } catch (BrokerException expected) {
            // The row is still pending; that is the point.
        }
        state(database, consumer);

        broker.setAvailable(true);
        say("broker restored");
        relay.publishPending();
        state(database, consumer);

        section("4. Relay crash after publishing, before marking: at-least-once delivery");
        OrderPlaced third = orders.placeOrder("cust-1003", new BigDecimal("7.25"));
        say("placed order " + third.orderId());
        relay.crashAfterNextPublish();
        try {
            relay.publishPending();
        } catch (OutboxRelay.SimulatedCrashException e) {
            say("relay: " + e.getMessage());
        }
        state(database, consumer);

        say("relay restarted");
        new OutboxRelay(database, broker, OutboxDemo::say).publishPending();
        state(database, consumer);

        section("5. Business transaction rolls back: no order, and no event");
        OrderPlaced doomed = OrderPlaced.create("cust-1004", new BigDecimal("500.00"));
        say("BEGIN");
        say("  INSERT INTO orders  (id=" + doomed.orderId() + ", total=" + doomed.total() + ", status=PLACED)");
        say("  INSERT INTO outbox  (message_id=" + doomed.messageId() + ", published_at=NULL)");
        try {
            orders.placeOrderAndFail(doomed);
        } catch (OrderService.OrderFailedException e) {
            say(e.getMessage() + " -> ROLLBACK");
        }
        state(database, consumer);

        section("Summary");
        say("  orders committed              : " + database.queryInt("SELECT COUNT(*) FROM orders"));
        say("  distinct events published     : "
                + database.queryInt("SELECT COUNT(*) FROM outbox WHERE published_at IS NOT NULL"));
        say("  messages accepted by broker   : " + broker.acceptedCount()
                + " (including redeliveries)");
        say("  shipments scheduled           : " + database.queryInt("SELECT COUNT(*) FROM shipments"));
        say("  duplicate deliveries ignored  : " + consumer.duplicateCount());
        say("  orders without an event       : " + database.queryInt("""
                SELECT COUNT(*) FROM orders o
                 WHERE NOT EXISTS (SELECT 1 FROM outbox x WHERE x.aggregate_id = o.id)"""));
        say("  events without an order       : " + database.queryInt("""
                SELECT COUNT(*) FROM outbox x
                 WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.id = x.aggregate_id)"""));
    }

    private static void runNaive(Database database, LocalBroker broker, ShipmentConsumer consumer)
            throws SQLException, BrokerException {
        section("Naive approach: publish first, then run the business transaction");
        NaiveOrderService naive = new NaiveOrderService(database, broker, OutboxDemo::say);
        String phantomOrderId = naive.placeOrderThenFail("cust-2001", new BigDecimal("99.99"));

        section("State afterwards");
        say("orders=" + database.queryInt("SELECT COUNT(*) FROM orders")
                + "  outbox=" + database.queryInt("SELECT COUNT(*) FROM outbox")
                + "  shipments=" + database.queryInt("SELECT COUNT(*) FROM shipments")
                + "  events published=" + broker.acceptedCount());
        say("The broker -- and the shipment consumer -- acted on order " + phantomOrderId
                + ", which does not exist in the database.");

        section("Result");
        say("The transactional outbox removes this failure mode: the event is written in the");
        say("same transaction as the order and published only after that transaction commits.");
        say("If the transaction rolls back, there is nothing for the relay to publish.");
    }

    private static void state(Database database, ShipmentConsumer consumer) throws SQLException {
        say("state: orders=" + database.queryInt("SELECT COUNT(*) FROM orders")
                + "  outbox pending=" + database.queryInt("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL")
                + "  outbox published=" + database.queryInt("SELECT COUNT(*) FROM outbox WHERE published_at IS NOT NULL")
                + "  shipments=" + database.queryInt("SELECT COUNT(*) FROM shipments")
                + "  duplicates ignored=" + consumer.duplicateCount());
    }

    private static void section(String title) {
        say();
        say("--- " + title + " ---");
    }

    private static void say(String message) {
        System.out.println(message);
    }

    private static void say() {
        System.out.println();
    }

    private OutboxDemo() {
    }
}
