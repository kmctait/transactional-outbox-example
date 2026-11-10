package com.example.outbox;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Creates orders. The business row and the {@code OrderPlaced} event are written
 * by the <em>same</em> local transaction, which is the whole point of the
 * transactional outbox: the event exists if and only if the order exists.
 */
final class OrderService {

    /** Carries the IDs of the work that was rolled back, so the demo can explain them. */
    static final class OrderFailedException extends RuntimeException {

        private final String orderId;
        private final String messageId;

        OrderFailedException(OrderPlaced event) {
            super("simulated downstream failure");
            this.orderId = event.orderId();
            this.messageId = event.messageId();
        }

        String orderId() {
            return orderId;
        }

        String messageId() {
            return messageId;
        }
    }

    private final Database database;

    OrderService(Database database) {
        this.database = database;
    }

    OrderPlaced placeOrder(String customerId, java.math.BigDecimal total) throws SQLException {
        OrderPlaced event = OrderPlaced.create(customerId, total);
        return placeOrder(event);
    }

    OrderPlaced placeOrder(OrderPlaced event) throws SQLException {
        database.inTransaction(connection -> {
            insertOrder(connection, event);
            insertOutboxRow(connection, event);
            return null;
        });
        return event;
    }

    /**
     * Writes the order and its event and then fails, to show that the rollback
     * discards both rows: no order and no event.
     */
    OrderPlaced placeOrderAndFail(OrderPlaced event) throws SQLException {
        try {
            database.inTransaction(connection -> {
                insertOrder(connection, event);
                insertOutboxRow(connection, event);
                throw new IllegalStateException("simulated downstream failure");
            });
        } catch (IllegalStateException e) {
            throw new OrderFailedException(event);
        }
        throw new AssertionError("unreachable: the transaction always fails");
    }

    static void insertOrder(Connection connection, OrderPlaced event) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO orders (id, customer_id, total, status, created_at)
                VALUES (?, ?, ?, ?, ?)""")) {
            statement.setString(1, event.orderId());
            statement.setString(2, event.customerId());
            statement.setBigDecimal(3, event.total());
            statement.setString(4, "PLACED");
            statement.setTimestamp(5, java.sql.Timestamp.from(event.createdAt()));
            statement.executeUpdate();
        }
    }

    static void insertOutboxRow(Connection connection, OrderPlaced event) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO outbox (message_id, aggregate_type, aggregate_id, event_type, payload, created_at)
                VALUES (?, ?, ?, ?, ?, ?)""")) {
            statement.setString(1, event.messageId());
            statement.setString(2, event.aggregateType());
            statement.setString(3, event.orderId());
            statement.setString(4, event.eventType());
            statement.setString(5, event.payload());
            statement.setTimestamp(6, java.sql.Timestamp.from(event.createdAt()));
            statement.executeUpdate();
        }
    }
}
