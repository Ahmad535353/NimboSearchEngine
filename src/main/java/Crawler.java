import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Crawler {
    final static int threadNumber = 64;
    final static int LruTimeLimit = 30;
    static AtomicInteger counter = new AtomicInteger(0);

    private static Logger logger = LoggerFactory.getLogger(Crawler.class);

    public static HashMap<String,Boolean> tempStorage = new HashMap<>();

    public static void main(String args[]) throws InterruptedException {

        Queue queue = new Queue(threadNumber);
        Elastic elasticEngine = new Elastic();
        LruCache cacheLoader = new LruCache(LruTimeLimit);



        //            **** Q ****
//        System.out.println("seed added");
        logger.info("Seed added.");
        Queue.add("https://en.wikipedia.org/wiki/Main_Page",0);
        Queue.add("https://us.yahoo.com/",1);
        Queue.add("https://www.nytimes.com/",2);
        Queue.add("https://www.msn.com/en-us/news",3);
        Queue.add("http://www.telegraph.co.uk/news/",4);
        Queue.add("http://www.alexa.com",5);
        Queue.add("http://www.apache.org",6);
        Queue.add("https://en.wikipedia.org/wiki/Main_Page/World_war_II",7);
        Queue.add("http://www.news.google.com",8);
        Queue.add("http://www.independent.co.uk",9);
//            **** Q ****

        long time = System.currentTimeMillis();
        ArrayList<ParserThread> threadList = new ArrayList<ParserThread>();
        ArrayList<StoreInQ> storeInQS = new ArrayList<StoreInQ>();

        for (int i = 0 ; i < threadNumber; i++){
            ParserThread parserThread = new ParserThread(cacheLoader, queue, elasticEngine, i);
            threadList.add(parserThread);
            logger.info("thread {} Started.",i);
        }

        for (int i = 0; i < threadNumber / 10; ++i) {
            StoreInQ storeInQ = new StoreInQ(i);
            storeInQS.add(storeInQ);
            storeInQ.start();
        }


        for (int i = 0 ; i < threadNumber ; i++){
            threadList.get(i).joinThread();
            logger.info("thread {} ended.",i);
        }

        for (int i = 0; i < threadNumber / 10; ++i) {
            storeInQS.get(i).stop();
        }
        time = System.currentTimeMillis() - time;
        logger.info("Atomic counter is {} ", counter);

    }
}