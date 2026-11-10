package com.example.outbox;

/** The message relay's view of the message broker. */
interface MessageBroker {

    /**
     * Publishes the message. Implementations must throw if the broker did not
     * durably accept it, so the relay knows to leave the outbox row unpublished.
     */
    void publish(OutboxMessage message) throws BrokerException;
}
