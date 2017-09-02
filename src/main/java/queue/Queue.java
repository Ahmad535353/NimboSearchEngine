package queue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.*;
import org.apache.kafka.clients.consumer.*;

import java.util.*;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

public class Queue {
    static ArrayBlockingQueue buffer;

    public Queue() throws InterruptedException {
        buffer = new ArrayBlockingQueue(100000);
    }

    public static String take() throws InterruptedException {
        //take from buffer
        String recordValue = (String)buffer.take();
        return recordValue;
    }

    public static void add(String topic, String value) {
        //add to kafka
        ProducerApp.send(topic, value);
    }
}