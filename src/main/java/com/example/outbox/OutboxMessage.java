package com.example.outbox;

import java.time.Instant;

/** One row of the {@code outbox} table. */
record OutboxMessage(
        long id,
        String messageId,
        String aggregateType,
        String aggregateId,
        String eventType,
        String payload,
        Instant createdAt) {
}
