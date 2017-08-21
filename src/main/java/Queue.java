import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.*;
import org.apache.kafka.clients.consumer.*;

import java.util.*;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;

public class Queue {
    public ArrayList<String> take(int threadNumber){
        //taking first element of queue
        ArrayList<String> urls = new ArrayList<>();
        ConsumerRecords<String, String> records = ConsumerApp.myConsumers[threadNumber].poll(1000);
        for (ConsumerRecord<String , String> record:records)
            urls.add(record.value().toString());
        return urls;
    }
    public void add(String newUrl , int threadNumber){
        //key , Integer.toString(i)
        ProducerApp.producers[threadNumber].send(new ProducerRecord<String, String>("my-10th-topic", newUrl + ""));
        //return queue.add(newUrl);
    }
    public Queue(int threadNumber){
        // ConsumerApp consumerApp = new ConsumerApp();
        ConsumerApp consumerApp = new ConsumerApp(threadNumber);
        ProducerApp producerApp = new ProducerApp(threadNumber);
        //queue = new ArrayBlockingQueue<String>(10000);
    }

}
