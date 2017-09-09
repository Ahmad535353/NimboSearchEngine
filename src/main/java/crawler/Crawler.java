package crawler;

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import elastic.Elastic;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.ConsumerApp;
import kafka.ProducerApp;
import utils.Constants;
import utils.Pair;
import utils.Statistics;

import java.util.concurrent.ArrayBlockingQueue;

public class Crawler {

//    public static Elastic elasticEngine;
    private static Logger logger;
    public static LruCache firstCache = new LruCache(Constants.LRU_TIME_LIMIT);
    public static LruCache secondCache = new LruCache(Constants.LRU_TIME_LIMIT);
    static {
        logger = LoggerFactory.getLogger(Crawler.class);
//        elasticEngine = new Elastic();
    }

    public static void main(String args[]) throws InterruptedException, LangDetectException {

//        DetectorFactory.loadProfile("/home/nimbo_search/amirphl/profiles");

        logger.info("Seed added.");
        ProducerApp.send(Constants.URL_TOPIC, "https://en.wikipedia.org/wiki/Main_Page");
        ProducerApp.send(Constants.URL_TOPIC, "https://us.yahoo.com/");
        ProducerApp.send(Constants.URL_TOPIC, "https://www.nytimes.com/");
        ProducerApp.send(Constants.URL_TOPIC, "https://www.msn.com/en-us/news");
        ProducerApp.send(Constants.URL_TOPIC, "http://www.telegraph.co.uk/news/");
        ProducerApp.send(Constants.URL_TOPIC, "http://www.alexa.com");
        ProducerApp.send(Constants.URL_TOPIC, "http://www.apache.org");
        ProducerApp.send(Constants.URL_TOPIC, "https://en.wikipedia.org/wiki/Main_Page/World_war_II");
        ProducerApp.send(Constants.URL_TOPIC, "http://www.news.google.com");
        ProducerApp.send(Constants.URL_TOPIC, "http://www.independent.co.uk");

        Statistics.getInstance().setThreadsNums(Constants.WORKER_THREAD_NUMBER);
        Thread stat = new Thread(Statistics.getInstance());
        stat.start();

        Thread manager = new Thread(new ThreadManager());

        for (int i = 0; i < Constants.WORKER_THREAD_NUMBER + Constants.PARSER_THREAD_NUMBER; i++) {
            new Thread(new WorkerThread(i)).start();
        }

//        for (int i = 0; i < Constants.PARSER_NUMBER; i++) {
//            new Thread(new Parser(i)).start();
//        }
//        for (int i = 0; i < Constants.FETCHER_NUMBER; i++) {
//            new Thread(new Fetcher(i)).start();
//        }

        ConsumerApp consumerApp = new ConsumerApp();
        consumerApp.start();

        Thread.sleep(20000);
        manager.start();
    }
}