package com.example.outbox;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.function.Consumer;

/**
 * The anti-pattern, for contrast: the event is published to the broker before
 * the business transaction completes. When the transaction then rolls back, the
 * rest of the system has already been told about an order that does not exist.
 *
 * <p>This is exactly the inconsistency the transactional outbox prevents.</p>
 */
final class NaiveOrderService {

    private final Database database;
    private final MessageBroker broker;
    private final Consumer<String> log;

    NaiveOrderService(Database database, MessageBroker broker) {
        this(database, broker, message -> { });
    }

    NaiveOrderService(Database database, MessageBroker broker, Consumer<String> log) {
        this.database = database;
        this.broker = broker;
        this.log = log;
    }

    /**
     * Publishes the event, then attempts the database write and fails it.
     *
     * @return the ID of the order that the broker was told about but that does
     *         not exist in the database
     */
    String placeOrderThenFail(String customerId, BigDecimal total) throws SQLException, BrokerException {
        OrderPlaced event = OrderPlaced.create(customerId, total);

        log.accept("naive: publishing " + event.eventType() + " directly to the broker, outside any transaction");
        broker.publish(event.toOutboxMessage());
        log.accept("naive: broker accepted the event (HTTP 202)");

        try {
            database.inTransaction(connection -> {
                OrderService.insertOrder(connection, event);
                throw new IllegalStateException("database write failed");
            });
        } catch (IllegalStateException ignored) {
            log.accept("naive: database write failed -> ROLLBACK (orders table unchanged)");
        }
        return event.orderId();
    }
}
