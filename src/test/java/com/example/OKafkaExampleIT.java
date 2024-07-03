package com.example;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import lombok.extern.slf4j.Slf4j;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.oracle.okafka.clients.consumer.KafkaConsumer;
import org.oracle.okafka.clients.producer.KafkaProducer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Running this test requires a Oracle Database container running locally, configured to use TEQ.
 * See the project README.md for steps on setting up the local test environment.
 */
@Slf4j
public class OKafkaExampleIT {
    private static final String testUser = "testuser";
    private static final String testPassword = "Welcome123#";
    private static final Integer localPort = 1521;
    private static PoolDataSource dataSource;
    private final String topicName = "TEQ_EXAMPLE";

    @BeforeAll
    static void setUp() throws SQLException {
        // Configure a datasource for the Oracle Database container
        dataSource = PoolDataSourceFactory.getPoolDataSource();
        dataSource.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
        dataSource.setConnectionPoolName("OKAFKA_SAMPLE");
        dataSource.setUser(testUser);
        dataSource.setPassword(testPassword);
        dataSource.setURL(String.format("jdbc:oracle:thin:@localhost:%d/freepdb1", localPort));
    }

    @Test
    void producerConsumerExample() throws Exception {
        // Create a topic to produce messages to, and consume messages from.
        // This topic will have 1 partition and a replication factor of 0, since we are testing locally with limited resources.
        NewTopic topic = new NewTopic(topicName, 1, (short) 0);
        log.info("Creating topic {}", topicName);
        AdminUtil.createTopicIfNotExists(getOKafkaConnectionProperties(), topic);

        // Create the OKafka Producer.
        Properties producerProps = getOKafkaConnectionProperties();
        producerProps.put("enable.idempotence", "true");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> okafkaProducer = new KafkaProducer<>(producerProps);

        // Create the OKafka Consumer.
        Properties consumerProps = getOKafkaConnectionProperties();
        consumerProps.put("group.id" , "MY_CONSUMER_GROUP");
        consumerProps.put("enable.auto.commit","false");
        consumerProps.put("max.poll.records", 2000);
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<String, String> okafkaConsumer = new KafkaConsumer<>(consumerProps);

        // Launch the sample producer and consumer asynchronously, and wait for their completion.
        try (SampleProducer<String> sampleProducer = new SampleProducer<>(okafkaProducer, topicName, getDataStream());
                SampleConsumer<String> sampleConsumer = new SampleConsumer<>(okafkaConsumer, topicName, 50);
                ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<?> producerFuture = executor.submit(sampleProducer);
            Future<?> consumerFuture = executor.submit(sampleConsumer);
            producerFuture.get();
            consumerFuture.get();
        }

        // Because the data produced by the OKafka Producer lands in TEQ, we can easily query this using SQL.
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("""
                        select * from TEQ_EXAMPLE
                        fetch first 1 row only
                    """);
            assertThat(resultSet.next());
            ResultSetMetaData metaData = resultSet.getMetaData();
            log.info("TEQ_EXAMPLE Columnds:");
            metaData.getColumnCount();
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                log.info(metaData.getColumnName(i + 1));
            }
        }
    }

    private Properties getOKafkaConnectionProperties() {
        String ojbdcFilePath = new File("src/test/resources").getAbsolutePath();
        return OKafkaProperties.getLocalConnectionProps(ojbdcFilePath, localPort);
    }

    private Stream<String> getDataStream() throws IOException {
        return Files.lines(new File("src/test/resources/weather_sensor_data.txt").toPath());
    }
}
