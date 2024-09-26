package com.example;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.oracle.okafka.clients.producer.KafkaProducer;

/**
 * TransactionalProducer uses the org.oracle.okafka.clients.producer.KafkaProducer database connection
 * to write records to a table.
 *
 * After limit records have been produced, the TransactionalProducer simulates a processing error,
 * and aborts the current producer batch.
 */
public class TransationalProducer implements Runnable, AutoCloseable {
    private final String insertRecord = """
            insert into records (data, idx) values (?, ?)
            """;

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final Stream<String> inputs;

    // Simulate an message processing error after limit messages have been produced
    private final int limit;

    public TransationalProducer(KafkaProducer<String, String> producer,
                                String topic,
                                Stream<String> inputs,
                                int limit) {
        this.producer = producer;
        this.topic = topic;
        this.inputs = inputs;
        this.limit = limit;
    }

    @Override
    public void run() {
        // initialize the producer and start a transaction
        producer.initTransactions();
        producer.beginTransaction();
        Connection conn = producer.getDBConnection();
        AtomicInteger i = new AtomicInteger(0);
        Iterator<String> records = inputs.iterator();
        while (records.hasNext()) {
            String record = records.next();
            int idx = i.getAndIncrement();
            if (idx >= limit) {
                System.out.println("Unexpected error processing records. Aborting transaction!");
                // Abort the database transaction on error, cancelling the effective batch
                producer.abortTransaction();
                return;
            }

            System.out.println("Produced record: " + record);
            ProducerRecord<String, String> pr = new ProducerRecord<>(topic, Integer.toString(idx), record);
            producer.send(pr);
            persistRecord(record, idx, conn);
        }
        // commit the transaction
        producer.commitTransaction();
    }

    private void persistRecord(String record, int idx, Connection conn) {
        try (PreparedStatement ps = conn.prepareStatement(insertRecord)) {
            ps.setString(1, record);
            ps.setInt(2, idx);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (this.producer != null) {
            producer.close();
        }
    }
}
