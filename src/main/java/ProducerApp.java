import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.producer.*;

import java.text.*;
import java.util.*;

public class ProducerApp {
    public static Producer<String, String> producers[];

    public ProducerApp(int threadNumber) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.16.80:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 100);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producers = new Producer[threadNumber];
        for (int i = 0 ; i < threadNumber; i++)
            producers[i] = new KafkaProducer<String, String>(props);
    }
}
