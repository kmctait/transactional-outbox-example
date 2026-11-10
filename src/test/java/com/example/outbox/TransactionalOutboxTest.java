package com.example.outbox;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransactionalOutboxTest {

    @Test
    void placeOrderCommitsBusinessRowAndOutboxRowTogether() throws Exception {
        try (Database database = Database.start()) {
            OrderService orders = new OrderService(database);

            OrderPlaced event = orders.placeOrder("cust-1", new BigDecimal("10.00"));

            assertEquals(1, database.queryInt("SELECT COUNT(*) FROM orders WHERE id = ?", event.orderId()));
            assertEquals(1, database.queryInt(
                    "SELECT COUNT(*) FROM outbox WHERE message_id = ? AND published_at IS NULL",
                    event.messageId()));
        }
    }

    @Test
    void rolledBackTransactionLeavesNoOrderAndNoEvent() throws Exception {
        try (Database database = Database.start()) {
            OrderService orders = new OrderService(database);
            OrderPlaced event = OrderPlaced.create("cust-1", new BigDecimal("10.00"));

            assertThrows(OrderService.OrderFailedException.class, () -> orders.placeOrderAndFail(event));

            assertEquals(0, database.queryInt("SELECT COUNT(*) FROM orders"));
            assertEquals(0, database.queryInt("SELECT COUNT(*) FROM outbox"));
        }
    }

    @Test
    void relayPublishesInOutboxOrderAndMarksRowsPublished() throws Exception {
        try (Database database = Database.start(); LocalBroker broker = LocalBroker.start()) {
            OrderService orders = new OrderService(database);
            OrderPlaced first = orders.placeOrder("cust-1", new BigDecimal("1.00"));
            OrderPlaced second = orders.placeOrder("cust-2", new BigDecimal("2.00"));

            OutboxRelay relay = new OutboxRelay(database, broker);

            assertEquals(2, relay.publishPending());
            assertEquals(List.of(first.messageId(), second.messageId()), broker.acceptedMessageIds());
            assertEquals(0, database.queryInt("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL"));
        }
    }

    @Test
    void relayDoesNotSeeRowsFromUncommittedTransactions() throws Exception {
        try (Database database = Database.start(); LocalBroker broker = LocalBroker.start()) {
            OrderPlaced event = OrderPlaced.create("cust-1", new BigDecimal("5.00"));
            CountDownLatch written = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();

            Thread writer = new Thread(() -> {
                try {
                    database.inTransaction(connection -> {
                        OrderService.insertOrder(connection, event);
                        OrderService.insertOutboxRow(connection, event);
                        written.countDown();
                        await(release);
                        return null;
                    });
                } catch (Throwable t) {
                    failure.set(t);
                }
            }, "open-transaction");
            writer.start();

            assertTrue(written.await(5, TimeUnit.SECONDS));
            try {
                // The row exists only inside the writer's open transaction.
                assertEquals(0, new OutboxRelay(database, broker).publishPending());
                assertEquals(0, broker.acceptedCount());
            } finally {
                release.countDown();
            }
            writer.join(TimeUnit.SECONDS.toMillis(5));
            assertNull(failure.get());

            assertEquals(1, new OutboxRelay(database, broker).publishPending());
            assertEquals(1, broker.acceptedCount());
        }
    }

    @Test
    void brokerOutageLeavesEventPendingAndRetrySucceeds() throws Exception {
        try (Database database = Database.start(); LocalBroker broker = LocalBroker.start()) {
            OrderService orders = new OrderService(database);
            ShipmentConsumer consumer = new ShipmentConsumer(database);
            broker.subscribe(consumer::onEvent);
            orders.placeOrder("cust-1", new BigDecimal("10.00"));

            broker.setAvailable(false);
            assertThrows(BrokerException.class, () -> new OutboxRelay(database, broker).publishPending());
            assertEquals(1, database.queryInt("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL"));
            assertEquals(0, database.queryInt("SELECT COUNT(*) FROM shipments"));

            broker.setAvailable(true);
            assertEquals(1, new OutboxRelay(database, broker).publishPending());
            assertEquals(0, database.queryInt("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL"));
            assertEquals(1, database.queryInt("SELECT COUNT(*) FROM shipments"));
        }
    }

    @Test
    void duplicateDeliveryIsIgnoredByTheConsumer() throws Exception {
        try (Database database = Database.start(); LocalBroker broker = LocalBroker.start()) {
            ShipmentConsumer consumer = new ShipmentConsumer(database);
            broker.subscribe(consumer::onEvent);
            OrderPlaced event = OrderPlaced.create("cust-1", new BigDecimal("3.00"));

            broker.publish(event.toOutboxMessage());
            broker.publish(event.toOutboxMessage());

            assertEquals(1, consumer.processedCount());
            assertEquals(1, consumer.duplicateCount());
            assertEquals(1, database.queryInt("SELECT COUNT(*) FROM shipments"));
            assertEquals(1, database.queryInt("SELECT COUNT(*) FROM processed_messages"));
        }
    }

    @Test
    void crashAfterPublishCausesRedeliveryWithoutDuplicateEffect() throws Exception {
        try (Database database = Database.start(); LocalBroker broker = LocalBroker.start()) {
            OrderService orders = new OrderService(database);
            ShipmentConsumer consumer = new ShipmentConsumer(database);
            broker.subscribe(consumer::onEvent);
            orders.placeOrder("cust-1", new BigDecimal("4.00"));

            OutboxRelay relay = new OutboxRelay(database, broker);
            relay.crashAfterNextPublish();
            assertThrows(OutboxRelay.SimulatedCrashException.class, relay::publishPending);

            assertEquals(1, broker.acceptedCount());
            assertEquals(1, database.queryInt("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL"));
            assertEquals(1, database.queryInt("SELECT COUNT(*) FROM shipments"));

            assertEquals(1, new OutboxRelay(database, broker).publishPending());

            assertEquals(2, broker.acceptedCount());
            assertEquals(0, database.queryInt("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL"));
            assertEquals(1, database.queryInt("SELECT COUNT(*) FROM shipments"));
            assertEquals(1, consumer.duplicateCount());
        }
    }

    @Test
    void naivePublishingLeavesAPhantomEventWhenTheTransactionRollsBack() throws Exception {
        try (Database database = Database.start(); LocalBroker broker = LocalBroker.start()) {
            ShipmentConsumer consumer = new ShipmentConsumer(database);
            broker.subscribe(consumer::onEvent);

            new NaiveOrderService(database, broker).placeOrderThenFail("cust-1", new BigDecimal("9.99"));

            assertEquals(0, database.queryInt("SELECT COUNT(*) FROM orders"));
            assertEquals(0, database.queryInt("SELECT COUNT(*) FROM outbox"));
            assertEquals(1, broker.acceptedCount());
            assertEquals(1, database.queryInt("SELECT COUNT(*) FROM shipments"));
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out waiting for the test to proceed");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
