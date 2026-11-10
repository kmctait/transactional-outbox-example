package com.example.outbox;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * The consumer on the other side of the broker. Because delivery is
 * at-least-once, the consumer records every processed message ID and ignores
 * duplicates in the same transaction that applies the effect.
 */
final class ShipmentConsumer {

    private final Database database;
    private final Consumer<String> log;
    private final AtomicInteger processed = new AtomicInteger();
    private final AtomicInteger duplicates = new AtomicInteger();

    ShipmentConsumer(Database database) {
        this(database, message -> { });
    }

    ShipmentConsumer(Database database, Consumer<String> log) {
        this.database = database;
        this.log = log;
    }

    /** Handles one broker delivery; safe to call more than once for the same message. */
    void onEvent(LocalBroker.Event event) {
        try {
            database.inTransaction(connection -> {
                if (alreadyProcessed(connection, event.messageId())) {
                    duplicates.incrementAndGet();
                    log.accept("consumer: message " + event.messageId()
                            + " already in processed_messages -> ignored (idempotent)");
                    return null;
                }
                insertShipment(connection, event.aggregateId());
                markProcessed(connection, event.messageId());
                processed.incrementAndGet();
                log.accept("consumer: shipment scheduled for order " + event.aggregateId()
                        + " (message " + event.messageId() + ")");
                return null;
            });
        } catch (SQLException e) {
            throw new IllegalStateException("consumer failed for message " + event.messageId(), e);
        }
    }

    private static boolean alreadyProcessed(Connection connection, String messageId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM processed_messages WHERE message_id = ?")) {
            statement.setString(1, messageId);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    private static void insertShipment(Connection connection, String orderId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO shipments (order_id, status, created_at) VALUES (?, ?, ?)")) {
            statement.setString(1, orderId);
            statement.setString(2, "SCHEDULED");
            statement.setTimestamp(3, Timestamp.from(Instant.now()));
            statement.executeUpdate();
        }
    }

    private static void markProcessed(Connection connection, String messageId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO processed_messages (message_id, processed_at) VALUES (?, ?)")) {
            statement.setString(1, messageId);
            statement.setTimestamp(2, Timestamp.from(Instant.now()));
            statement.executeUpdate();
        }
    }

    int processedCount() {
        return processed.get();
    }

    int duplicateCount() {
        return duplicates.get();
    }
}
