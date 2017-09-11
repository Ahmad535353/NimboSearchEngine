package crawler;

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.ConsumerApp;
import kafka.ProducerApp;
import utils.Constants;
import utils.Statistics;

public class Crawler {

//    public static Elastic elasticEngine;
    private static Logger logger;
    public static LruCache firstCache = new LruCache(Constants.LRU_TIME_LIMIT);
    public static LruCache secondCache = new LruCache(Constants.LRU_TIME_LIMIT);
    static {
        logger = LoggerFactory.getLogger(Crawler.class);
//        elasticEngine = Elastic.getInstance();
    }

    public static void main(String args[]) throws InterruptedException, LangDetectException {

        DetectorFactory.loadProfile("/home/nimbo_search/profiles");

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


        int totalThreadNum = Constants.EXAMINE_THREAD_NUMBER + Constants.FETCHER_THREAD_NUMBER
                + Constants.PARSER_THREAD_NUMBER + Constants.HBASE_THREAD_NUMBER + Constants.ELASTIC_THREAD_NUMBER
                + Constants.KAFKA_THREAD_NUMBER;
        ConsumerApp consumerApp = new ConsumerApp();
        consumerApp.start();

        Statistics.getInstance().setThreadsNums(totalThreadNum);
        Thread stat = new Thread(Statistics.getInstance());
        stat.start();

        Thread manager = new Thread(new ThreadManager());

//        for (int i = 0; i < Constants.WORKER_THREAD_NUMBER + Constants.PARSER_THREAD_NUMBER; i++) {
//            new Thread(new WorkerThread(i)).start();
//        }
        int i = 0;
        for (int j = 0; j < Constants.EXAMINE_THREAD_NUMBER; j++, i++) {
            new Thread(new WorkerThread(i, 1)).start();
        }
        for (int j = 0; j < Constants.FETCHER_THREAD_NUMBER; j++, i++) {
            new Thread(new WorkerThread(i, 2)).start();
        }
        for (int j = 0; j < Constants.PARSER_THREAD_NUMBER; j++, i++) {
            new Thread(new WorkerThread(i, 3)).start();
        }
        for (int j = 0; j < Constants.HBASE_THREAD_NUMBER; j++, i++) {
            new Thread(new WorkerThread(i, 4)).start();
        }
        for (int j = 0; j < Constants.ELASTIC_THREAD_NUMBER; j++, i++) {
            new Thread(new WorkerThread(i, 5)).start();
        }
        for (int j = 0; j < Constants.KAFKA_THREAD_NUMBER; j++, i++) {
            new Thread(new WorkerThread(i, 6)).start();
        }


//        for (int i = 0; i < Constants.PARSER_NUMBER; i++) {
//            new Thread(new Parser(i)).start();
//        }
//        for (int i = 0; i < Constants.FETCHER_NUMBER; i++) {
//            new Thread(new Fetcher(i)).start();
//        }


//        Thread.sleep(20000);
//        manager.start();
    }
}