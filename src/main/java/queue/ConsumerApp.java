package queue;

import crawler.Crawler;
import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Constants;

import java.util.*;

public class ConsumerApp extends Thread {
//    private static Logger logger = LoggerFactory.getLogger(crawler.Crawler.class);
    public static KafkaConsumer<String, String> consumer;

    static {
        Properties props = new Properties();
        props.put("bootstrap.servers", "server1:9092, server2:9092");
        //props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("fetch.min.bytes", 1);
        props.put("group.id", "test");
        props.put("heartbeat.interval.ms", 3000);
        props.put("max.partition.fetch.bytes", 1048576);
        props.put("session.timeout.ms", 30000);
        props.put("auto.offset.reset", "earliest");
        props.put("connections.max.idle.ms", 540000);
        props.put("enable.auto.commit", true);
        props.put("exclude.internal.topics", true);
        props.put("max.poll.records", 100);
        props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        props.put("request.timeout.ms", 40000);
        props.put("auto.commit.interval.ms", 5000);
        props.put("fetch.max.wait.ms", 500);
        props.put("metadata.max.age.ms", 300000);
        props.put("reconnect.backoff.ms", 50);
        props.put("retry.backoff.ms", 100);
        props.put("client.id", "");
        consumer = new KafkaConsumer<String, String>(props);

        ArrayList<String> topics = new ArrayList<String>();
        topics.add(Constants.URL_TOPIC);
        consumer.subscribe(topics);
    }


    public void run () {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String , String> record:records) {
                try {
                    Crawler.urlQueue.put(record.value().toString());
                } catch (IllegalStateException e) {
//                    logger.error("{}", e.getMessage());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
