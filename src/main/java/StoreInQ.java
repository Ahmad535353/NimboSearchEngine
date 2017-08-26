import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoreInQ extends Thread {
    private int threadNumber;
    private static Logger logger = LoggerFactory.getLogger(Crawler.class);

    public void run () {
        while (true) {

            ConsumerRecords<String, String> records = ConsumerApp.myConsumers.poll(1000);
            for (ConsumerRecord<String , String> record:records)
                try {
                    while (Queue.buffers.remainingCapacity() == 0){
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    Queue.buffers.add(record.value().toString());
                }catch (IllegalStateException e ){
                    logger.error("{}",e.getMessage());
                }
        }
    }

    public StoreInQ (int threadNumber) {
        this.threadNumber = threadNumber;
    }
}
