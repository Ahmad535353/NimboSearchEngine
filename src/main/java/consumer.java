import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class conumer {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.16.80:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        ArrayList<String> topics = new ArrayList<>();
        topics.add("my-topic");
        consumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            consumer.seek("0" , 100);

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }
}
