package com.example.outbox;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A tiny message broker exposed over real HTTP on an ephemeral local port.
 *
 * <p>Keeping the publish hop as an actual network call makes the outbox's job
 * obvious: the database transaction and the broker are genuinely separate
 * systems, and either one can fail independently. The broker can be marked
 * unavailable to simulate an outage, and accepted messages are handed to the
 * registered subscriber (the shipment consumer).</p>
 */
final class LocalBroker implements MessageBroker, AutoCloseable {

    /** A message as delivered to a consumer. */
    record Event(String messageId, String eventType, String aggregateId, String payload) {
    }

    private final HttpServer server;
    private final ExecutorService executor;
    private final HttpClient client;
    private final AtomicInteger accepted = new AtomicInteger();
    private final List<String> acceptedMessageIds = Collections.synchronizedList(new ArrayList<>());

    private volatile boolean available = true;
    private volatile Consumer<Event> subscriber = event -> { };

    private LocalBroker(HttpServer server, ExecutorService executor) {
        this.server = server;
        this.executor = executor;
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();
    }

    static LocalBroker start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        LocalBroker broker = new LocalBroker(
                server,
                Executors.newSingleThreadExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "local-broker");
                    thread.setDaemon(true);
                    return thread;
                }));
        server.createContext("/events", broker::handlePublish);
        server.setExecutor(broker.executor);
        server.start();
        return broker;
    }

    /** Sends the message over HTTP and fails unless the broker answers 202 Accepted. */
    @Override
    public void publish(OutboxMessage message) throws BrokerException {
        HttpRequest request = HttpRequest.newBuilder(eventsUri())
                .timeout(Duration.ofSeconds(2))
                .header("Content-Type", "application/json")
                .header("X-Message-Id", message.messageId())
                .header("X-Event-Type", message.eventType())
                .header("X-Aggregate-Id", message.aggregateId())
                .POST(HttpRequest.BodyPublishers.ofString(message.payload()))
                .build();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 202) {
                throw new BrokerException("broker replied HTTP " + response.statusCode()
                        + " (" + response.body() + ")");
            }
        } catch (IOException e) {
            throw new BrokerException("broker unreachable: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BrokerException("interrupted while publishing", e);
        }
    }

    private void handlePublish(HttpExchange exchange) throws IOException {
        try {
            if (!"POST".equals(exchange.getRequestMethod())) {
                respond(exchange, 405, "method not allowed");
                return;
            }
            if (!available) {
                respond(exchange, 503, "broker is unavailable");
                return;
            }

            String payload = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            String messageId = exchange.getRequestHeaders().getFirst("X-Message-Id");
            String eventType = exchange.getRequestHeaders().getFirst("X-Event-Type");
            String aggregateId = exchange.getRequestHeaders().getFirst("X-Aggregate-Id");

            Event event = new Event(messageId, eventType, aggregateId, payload);
            accepted.incrementAndGet();
            acceptedMessageIds.add(messageId);
            subscriber.accept(event);

            respond(exchange, 202, "accepted " + messageId);
        } finally {
            exchange.close();
        }
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/plain");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private URI eventsUri() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/events");
    }

    void setAvailable(boolean available) {
        this.available = available;
    }

    void subscribe(Consumer<Event> subscriber) {
        this.subscriber = subscriber;
    }

    int acceptedCount() {
        return accepted.get();
    }

    List<String> acceptedMessageIds() {
        synchronized (acceptedMessageIds) {
            return List.copyOf(acceptedMessageIds);
        }
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }
}
