package com.example.outbox;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * The domain event written to the outbox when an order is placed. The same
 * record is used by the naive service, which publishes it without an outbox.
 */
record OrderPlaced(String messageId, String orderId, String customerId, BigDecimal total, Instant createdAt) {

    static OrderPlaced create(String customerId, BigDecimal total) {
        return new OrderPlaced(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                customerId,
                total,
                Instant.now());
    }

    String eventType() {
        return "OrderPlaced";
    }

    String aggregateType() {
        return "Order";
    }

    String payload() {
        return "{\"messageId\":\"%s\",\"orderId\":\"%s\",\"customerId\":\"%s\",\"total\":%s,\"occurredAt\":\"%s\"}"
                .formatted(messageId, orderId, customerId, total.toPlainString(), createdAt);
    }

    OutboxMessage toOutboxMessage() {
        return new OutboxMessage(0L, messageId, aggregateType(), orderId, eventType(), payload(), createdAt);
    }
}
