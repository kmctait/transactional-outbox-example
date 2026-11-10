package com.example.outbox;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * The message relay: a polling publisher.
 *
 * <p>It reads unpublished rows from the outbox in insertion order, publishes
 * each one, and only then records that it was published. If publishing fails,
 * the row stays pending and is retried later. If the relay crashes between
 * publishing and recording (which this class can simulate), the message is
 * published again after restart: delivery is at-least-once, so consumers must
 * be idempotent.</p>
 */
final class OutboxRelay {

    /** Pretends the relay process died after publishing but before marking the row. */
    static final class SimulatedCrashException extends RuntimeException {
        SimulatedCrashException(String message) {
            super(message);
        }
    }

    private static final int BATCH_SIZE = 100;

    private final Database database;
    private final MessageBroker broker;
    private final Consumer<String> log;
    private boolean crashAfterNextPublish;

    OutboxRelay(Database database, MessageBroker broker) {
        this(database, broker, message -> { });
    }

    OutboxRelay(Database database, MessageBroker broker, Consumer<String> log) {
        this.database = database;
        this.broker = broker;
        this.log = log;
    }

    /** Arms the crash simulation for the next successfully published message. */
    void crashAfterNextPublish() {
        this.crashAfterNextPublish = true;
    }

    /**
     * Publishes all pending messages in outbox order.
     *
     * @return the number of messages published and marked in this run
     */
    int publishPending() throws SQLException, BrokerException {
        List<OutboxMessage> pending = loadPending(BATCH_SIZE);
        if (pending.isEmpty()) {
            log.accept("relay: no pending messages");
            return 0;
        }

        int published = 0;
        for (OutboxMessage message : pending) {
            log.accept("relay: publishing " + message.eventType() + " " + message.messageId()
                    + " (aggregate=" + message.aggregateId() + ")");
            try {
                broker.publish(message);
            } catch (BrokerException e) {
                log.accept("relay: publish failed (" + e.getMessage()
                        + ") -> row stays pending for a later retry");
                throw e;
            }
            log.accept("relay: broker accepted the message (HTTP 202)");

            if (crashAfterNextPublish) {
                crashAfterNextPublish = false;
                throw new SimulatedCrashException(
                        "simulated crash before marking " + message.messageId() + " as published");
            }

            markPublished(message.id());
            log.accept("relay: marked " + message.messageId() + " as published");
            published++;
        }
        return published;
    }

    List<OutboxMessage> loadPending(int limit) throws SQLException {
        String sql = """
                SELECT id, message_id, aggregate_type, aggregate_id, event_type, payload, created_at
                  FROM outbox
                 WHERE published_at IS NULL
                 ORDER BY id
                 LIMIT ?""";
        try (Connection connection = database.connection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, limit);
            try (ResultSet resultSet = statement.executeQuery()) {
                List<OutboxMessage> messages = new ArrayList<>();
                while (resultSet.next()) {
                    messages.add(new OutboxMessage(
                            resultSet.getLong("id"),
                            resultSet.getString("message_id"),
                            resultSet.getString("aggregate_type"),
                            resultSet.getString("aggregate_id"),
                            resultSet.getString("event_type"),
                            resultSet.getString("payload"),
                            resultSet.getTimestamp("created_at").toInstant()));
                }
                return messages;
            }
        }
    }

    private void markPublished(long id) throws SQLException {
        String sql = "UPDATE outbox SET published_at = ? WHERE id = ? AND published_at IS NULL";
        try (Connection connection = database.connection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, Timestamp.from(Instant.now()));
            statement.setLong(2, id);
            statement.executeUpdate();
        }
    }
}
