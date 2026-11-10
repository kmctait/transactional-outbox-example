package com.example.outbox;

/** Raised when the broker is unreachable or refuses to accept a message. */
class BrokerException extends Exception {

    BrokerException(String message) {
        super(message);
    }

    BrokerException(String message, Throwable cause) {
        super(message, cause);
    }
}
