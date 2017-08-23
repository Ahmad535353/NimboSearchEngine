import org.apache.kafka.common.*;
import org.apache.kafka.clients.consumer.*;

import java.util.*;

public class ConsumerApp {
    public static KafkaConsumer<String, String>[] myConsumers;

    public ConsumerApp (int threadNumber) {
        myConsumers = new KafkaConsumer[threadNumber];

        // Create the Properties class to instantiate the Consumer with the desired settings:
        Properties props = new Properties();
        props.put("bootstrap.servers", "176.31.102.177:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("fetch.min.bytes", 1);
        props.put("group.id", "test");
        props.put("heartbeat.interval.ms", 3000);
        props.put("max.partition.fetch.bytes", 1048576);
        props.put("session.timeout.ms", 30000);
        props.put("auto.offset.reset", "latest");
        props.put("connections.max.idle.ms", 540000);
        props.put("enable.auto.commit", true);
        props.put("exclude.internal.topics", true);
        props.put("max.poll.records", 50);
        props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        props.put("request.timeout.ms", 40000);
        props.put("auto.commit.interval.ms", 5000);
        props.put("fetch.max.wait.ms", 500);
        props.put("metadata.max.age.ms", 300000);
        props.put("reconnect.backoff.ms", 50);
        props.put("retry.backoff.ms", 100);
        props.put("client.id", "");

        ArrayList<String> topics = new ArrayList<String>();
        topics.add("my-22th-topic");

        // Create a KafkaConsumer instance and configure it with properties.
        for (int i = 0; i < threadNumber; ++i) {
            myConsumers[i] = new KafkaConsumer<String, String>(props);
            myConsumers[i].subscribe(topics);
        }

    }
}
