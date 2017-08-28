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
    final static int threadNumber = 300;
    final static int LruTimeLimit = 30;
    final static String urlTopic = "fuckUBitch";
    final static String forParseDataTopic = "motherFucker";
    private static Logger logger = LoggerFactory.getLogger(Crawler.class);

    public static HashMap<String,Boolean> tempStorage = new HashMap<>();

    public static void main(String args[]) throws InterruptedException {
        Queue queue = new Queue();
        Elastic elasticEngine = new Elastic();
        LruCache cacheLoader = new LruCache(LruTimeLimit);

//        It'll be handled soon
        Queue.add(urlTopic, "https://en.wikipedia.org/wiki/Main_Page");
        Queue.add(urlTopic, "https://us.yahoo.com/");
        Queue.add(urlTopic, "https://www.nytimes.com/");
        Queue.add(urlTopic, "https://www.msn.com/en-us/news");
        Queue.add(urlTopic, "http://www.telegraph.co.uk/news/");
        Queue.add(urlTopic, "http://www.alexa.com");
        Queue.add(urlTopic, "http://www.apache.org");
        Queue.add(urlTopic, "https://en.wikipedia.org/wiki/Main_Page/World_war_II");
        Queue.add(urlTopic, "http://www.news.google.com");
        Queue.add(urlTopic, "http://www.independent.co.uk");
        logger.info("Seed added.");
//

        long time = System.currentTimeMillis();
        ArrayList<ParserThread> threadList = new ArrayList<ParserThread>();

        for (int i = 0 ; i < threadNumber; i++){
            ParserThread parserThread = new ParserThread(cacheLoader, queue, elasticEngine, i);
            threadList.add(parserThread);
            logger.info("thread {} Started.",i);
        }

        ConsumerApp consumerApp = new ConsumerApp();
        consumerApp.start();

        for (int i = 0 ; i < threadNumber ; i++){
            threadList.get(i).joinThread();
            logger.info("thread {} ended.",i);
        }
        time = System.currentTimeMillis() - time;

        consumerApp.stop();

        System.out.println(time / 1000 + "\n");
    }
}