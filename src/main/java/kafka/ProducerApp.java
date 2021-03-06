package kafka;

import org.apache.kafka.clients.producer.*;

import java.util.*;

public class ProducerApp {

    private static Producer<String, String> producer;

    static {
        Properties props = new Properties();
        props.put("bootstrap.servers", "server1:9092, server2:9092");
//        props.put("bootstrap.servers", "172.16.16.106:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 100);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }

    public static void send (String topic, String value) {
        producer.send(new ProducerRecord<String, String>(topic, value));
    }
}
