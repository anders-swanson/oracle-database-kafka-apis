package com.example;

import java.util.stream.Stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class SampleProducer<T> implements Runnable, AutoCloseable {
    private final Producer<String, T> producer;
    private final String topic;
    private final Stream<T> inputs;

    public SampleProducer(Producer<String, T> producer, String topic, Stream<T> inputs) {
        this.producer = producer;
        this.topic = topic;
        this.inputs = inputs;
    }

    @Override
    public void run() {
        inputs.forEach(t -> {
            log.info("produced record: {}", t);
            producer.send(new ProducerRecord<>(topic, t));
        });
    }

    @Override
    public void close() throws Exception {
        if (this.producer != null) {
            producer.close();
        }
    }
}
