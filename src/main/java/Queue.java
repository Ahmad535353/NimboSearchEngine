import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.*;
import org.apache.kafka.clients.consumer.*;

import java.util.*;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

class Queue {
    static ArrayBlockingQueue buffers;

    static String take(int threadNumber) throws InterruptedException {
        //taking first element of buffer
        String recordValue = (String) buffers.take();
        return recordValue;
    }
    static void add(String newUrl, int threadNumber) {
        //key , Integer.toString(i)
        ProducerApp.producers[threadNumber].send(new ProducerRecord<String, String>("my-50th-topic", newUrl + ""));
        //return queue.add(newUrl);
    }
//    public String take(int threadNumber){
//        //temp take
//        String url = "";
//        ConsumerRecords<String, String> records = ConsumerApp.myConsumers[threadNumber].poll(1000);
//        for (ConsumerRecord<String , String> record:records)
//            url = record.value().toString();
//        return url;
//    }


    Queue(int threadNumber) throws InterruptedException {
        ProducerApp producerApp = new ProducerApp(threadNumber);
        ConsumerApp consumerApp = new ConsumerApp(threadNumber / 10);
        buffers = new ArrayBlockingQueue(1000);
    }
}
