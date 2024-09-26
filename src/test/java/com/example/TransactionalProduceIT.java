package com.example;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import oracle.jdbc.pool.OracleDataSource;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.oracle.okafka.clients.producer.KafkaProducer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.MountableFile;

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionalProduceIT {
    // Oracle Databse 23ai Free container image
    private static final String oracleImage = "gvenzl/oracle-free:23.5-slim-faststart";
    private static final String testUser = "testuser";
    private static final String testPassword = "Welcome123#";
    private final String topicName = "TEST";

    private static OracleDataSource dataSource;

    @BeforeAll
    static void setUp() throws Exception {
        // Configure the Oracle Database container with the TxEventQ test user.
        oracleContainer.start();
        oracleContainer.copyFileToContainer(MountableFile.forClasspathResource("okafka.sql"), "/tmp/init.sql");
        oracleContainer.execInContainer("sqlplus", "sys / as sysdba", "@/tmp/init.sql");

        // Configure a datasource for the Oracle Database container.
        // The datasource is used to demonstrate TxEventQ table duality.
        dataSource = new OracleDataSource();
        dataSource.setUser(testUser);
        dataSource.setPassword(testPassword);
        dataSource.setURL(oracleContainer.getJdbcUrl());

        // Create test table for Transactional Producer
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement()) {
            stmt.executeQuery("""
                create table records (
                    id   varchar(36) default sys_guid() primary key,
                    data varchar(255),
                    idx  number
                )
                """);
        }
    }

    @Container
    private static final OracleContainer oracleContainer = new OracleContainer(oracleImage)
            .withStartupTimeout(Duration.ofMinutes(3)) // allow possible slow startup
            .withUsername(testUser)
            .withPassword(testPassword);

    @Test
    void transactionalProduce() throws Exception {
        // Create a topic to produce messages to, and consume messages from.
        // This topic will have 1 partition and a replication factor of 0,
        // since we are testing locally with a containerized database.
        NewTopic topic = new NewTopic(topicName, 1, (short) 0);
        AdminUtil.createTopicIfNotExists(getOKafkaConnectionProperties(), topic);

        // Create the KafkaProducer with Oracle Database connectivity information.
        Properties producerProps = getOKafkaConnectionProperties();
        producerProps.put("enable.idempotence", "true");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // This property is required for transactional producers
        producerProps.put("oracle.transactional.producer", "true");
        KafkaProducer<String, String> okafkaProducer = new KafkaProducer<>(producerProps);

        // The producer will process 15 records before failing,
        // aborting the transaction.
        try (TransationalProducer producer = new TransationalProducer(
                okafkaProducer,
                topicName,
                15)) {
            producer.produce(getDataStream());
        }

        System.out.println("#### TransactionalProducer completed ####");

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("""
                        select * from records
                    """);
            // Verify that no data were persisted to the records table
            assertThat(resultSet.next()).isFalse();
            System.out.println("#### Verified no records committed ####");
        }
    }

    private Properties getOKafkaConnectionProperties() {
        String ojbdcFilePath = new File("src/test/resources").getAbsolutePath();
        return OKafkaProperties.getLocalConnectionProps(ojbdcFilePath, oracleContainer.getOraclePort());
    }

    private Stream<String> getDataStream() throws IOException {
        return Files.lines(new File("src/test/resources/weather_sensor_data.txt").toPath());
    }
}
