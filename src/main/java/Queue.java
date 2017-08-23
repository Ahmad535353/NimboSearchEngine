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
    public static ArrayBlockingQueue[] buffers;

    public static String take(int threadNumber) throws InterruptedException {
        //taking first element of buffer
        String recordValue = (String) buffers[threadNumber / 10].take();
        return recordValue;
    }

//    public String take(int threadNumber){
//        //temp take
//        String url = "";
//        ConsumerRecords<String, String> records = ConsumerApp.myConsumers[threadNumber].poll(1000);
//        for (ConsumerRecord<String , String> record:records)
//            url = record.value().toString();
//        return url;
//    }

    public static void add(String newUrl , int threadNumber){
        //sending data to producer
        ProducerApp.producers[threadNumber].send(new ProducerRecord<String, String>("my-12th-topic", newUrl + ""));
    }

    public Queue(int threadNumber) throws InterruptedException {
        ProducerApp producerApp = new ProducerApp(threadNumber);
        ConsumerApp consumerApp = new ConsumerApp(threadNumber / 10);
        buffers = new ArrayBlockingQueue[threadNumber / 10];
        for (int i = 0; i < threadNumber / 10; ++i)
            buffers[i] = new ArrayBlockingQueue<String>(10000);
    }
}
