package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.impl.AMQConnection;
import com.rabbitmq.client.impl.DefaultExceptionHandler;
import com.rabbitmq.client.impl.LongStringHelper;
import com.rabbitmq.client.impl.Version;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MockConnectionTest {

    @Test
    void connectionParams_are_default_ones() {
        Connection connection = new MockConnectionFactory().newConnection();

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(connection.getAddress().getHostAddress()).isEqualTo("127.0.0.1");
        softly.assertThat(connection.getPort()).isEqualTo(ConnectionFactory.DEFAULT_AMQP_PORT);
        softly.assertThat(connection.getChannelMax()).isEqualTo(0);
        softly.assertThat(connection.getFrameMax()).isEqualTo(0);
        softly.assertThat(connection.getHeartbeat()).isEqualTo(0);
        softly.assertThat(connection.getClientProperties()).isEqualTo(AMQConnection.defaultClientProperties());
        softly.assertThat(connection.getClientProvidedName()).isNull();
        softly.assertThat(connection.getServerProperties().get("version"))
            .isEqualTo(LongStringHelper.asLongString(new Version(AMQP.PROTOCOL.MAJOR, AMQP.PROTOCOL.MINOR).toString()));
        softly.assertThat(connection.getExceptionHandler()).isExactlyInstanceOf(DefaultExceptionHandler.class);
        softly.assertAll();
    }

    @Test
    void close_closes_connection() throws IOException {
        try (Connection connection = new MockConnectionFactory().newConnection()) {
            connection.close();
            assertThat(connection.isOpen()).isFalse();
        }
    }

    @Test
    void close_with_timeout_closes_connection() throws IOException {
        try (Connection connection = new MockConnectionFactory().newConnection()) {
            connection.close(10);
            assertThat(connection.isOpen()).isFalse();
        }
    }

    @Test
    void abort_closes_connection() throws IOException {
        try (Connection connection = new MockConnectionFactory().newConnection()) {
            connection.abort();
            assertThat(connection.isOpen()).isFalse();
        }
    }

    @Test
    void abort_with_timeout_closes_connection() throws IOException {
        try (Connection connection = new MockConnectionFactory().newConnection()) {
            connection.abort(15);
            assertThat(connection.isOpen()).isFalse();
        }
    }

    @Test
    void blockedListeners_and_shutdown_listeners_are_not_stored() throws IOException {
        try (Connection connection = new MockConnectionFactory().newConnection()) {
            assertThat(connection.removeBlockedListener(null)).isTrue();
            assertThat(connection.addBlockedListener(null, null)).isNull();
            connection.clearBlockedListeners();
            connection.addShutdownListener(null);
            connection.removeShutdownListener(null);
            assertThat(connection.getCloseReason()).isNull();
        }
    }

    @Test
    void id_is_null_before_being_set() throws IOException {
        try (Connection connection = new MockConnectionFactory().newConnection()) {
            assertThat(connection.getId()).isNull();
            String id = UUID.randomUUID().toString();
            connection.setId(id);

            assertThat(connection.getId()).isEqualTo(id);
        }
    }

    @Test
    void createChannel_throws_when_connection_is_closed() throws IOException {
        try (Connection connection = new MockConnectionFactory().newConnection()) {
            connection.close();

            assertThatExceptionOfType(AlreadyClosedException.class)
                .isThrownBy(() -> connection.createChannel());
        }
    }

    @Test
    void protectedApiMethods_throw() throws IOException {
        try (Connection connection = new MockConnectionFactory().newConnection()) {
            assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> connection.notifyListeners());
        }
    }

    private static final String EXCHANGE = "exchange";
    private static final String QUEUE = "queue";

    @Test
    void exclusive_queue_cleanup() throws Exception {
        MockConnectionFactory factory = new MockConnectionFactory();

        try (Connection conn = factory.newConnection()) {
            createQueueAndPublish(conn, true);
        }

        try (Connection conn2 = factory.newConnection()) {
            assertThrows(IllegalArgumentException.class, () -> {
                readFromQueue(conn2);
            });
        }
    }

    @Test
    void autoDelete_queue_cleanup() throws Exception {
        MockConnectionFactory factory = new MockConnectionFactory();

        try (Connection conn = factory.newConnection()) {
            createQueueAndPublish(conn, false);
            CompletableFuture<Void> future = readFromQueue(conn);
            future.get(1000, TimeUnit.MILLISECONDS);
        }

        try (Connection conn2 = factory.newConnection()) {
            assertThrows(IllegalArgumentException.class, () -> {
                readFromQueue(conn2);
            });
        }
    }

    @Test
    void multiConnection_overlapping() throws Exception {
        MockConnectionFactory factory = new MockConnectionFactory();

        Connection conn = factory.newConnection();

        try (Connection conn2 = factory.newConnection()) {
            createQueueAndPublish(conn2, false);
        }

        CompletableFuture<Void> future = readFromQueue(conn);
        future.get(1000, TimeUnit.MILLISECONDS);
    }

    @Test
    void multiConnection_sequential() throws Exception {
        MockConnectionFactory factory = new MockConnectionFactory();

        try (Connection conn = factory.newConnection()) {
            createQueueAndPublish(conn, false);
        }

        try (Connection conn2 = factory.newConnection()) {
            CompletableFuture<Void> future = readFromQueue(conn2);
            future.get(1000, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void singleConnection() throws Exception {
        MockConnectionFactory factory = new MockConnectionFactory();

        try (Connection conn = factory.newConnection()) {
            createQueueAndPublish(conn, false);

            CompletableFuture<Void> future = readFromQueue(conn);
            future.get(1000, TimeUnit.MILLISECONDS);
        }
    }

    private void createQueueAndPublish(Connection conn, boolean exclusive) throws IOException {
        Channel channel = conn.createChannel();

        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true);
        channel.queueDeclare(QUEUE, false, exclusive, true, Map.of());
        channel.queueBind(QUEUE, EXCHANGE, "");

        channel.basicPublish(EXCHANGE, "", new AMQP.BasicProperties(), "hello".getBytes());
    }

    private CompletableFuture<Void> readFromQueue(Connection conn) throws IOException {
        CompletableFuture<Void> future = new CompletableFuture<Void>();
        Channel channel = conn.createChannel();

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                future.complete(null);
                super.handleDelivery(consumerTag, envelope, properties, body);
            }
        };

        channel.basicConsume(QUEUE, true, consumer);

        return future;
    }
}
