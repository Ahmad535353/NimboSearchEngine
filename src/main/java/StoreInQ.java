import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class StoreInQ extends Thread {
    private int threadNumber;

    public void run () {
        while (true) {
            ConsumerRecords<String, String> records = ConsumerApp.myConsumers[threadNumber].poll(1000);
            for (ConsumerRecord<String , String> record:records)
                Queue.buffers[threadNumber].add(record.value().toString());
        }
    }

    public StoreInQ (int threadNumber) {
        this.threadNumber = threadNumber;
    }
}
