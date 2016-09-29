package ru.nw.spiiras.nv;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Created by Nikolay_Vasilishin on 9/29/2016.
 */
public class KafkaTrafficProducer {

    private final Producer<Object, String> producer;

    KafkaTrafficProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "master:6667");
        props.put("acks", "0"); //TODO check
//        props.put("retries", 0);
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    public Future<RecordMetadata> send(String topic, String message) {
        return producer.send(new ProducerRecord<>(topic, message));
    }

    public void close() {
        producer.close();
    }
}
