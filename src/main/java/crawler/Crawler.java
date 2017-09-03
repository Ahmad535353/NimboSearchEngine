package crawler;

import elastic.Elastic;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import queue.ConsumerApp;
import queue.ProducerApp;
import utils.Constants;
import utils.MyEntry;
import utils.Statistics;

import java.util.concurrent.ArrayBlockingQueue;

public class Crawler {
//    public static Queue queue;
//    private final static int threadNumber = 30;
//    private final static int parserNumber = 60;
//    private final static int fetcherNumber = 10;
//    public static HBaseSample storage ;
//    public static HBase storage;

    public static Elastic elasticEngine ;
    public static ArrayBlockingQueue<String> urlQueue = new ArrayBlockingQueue<String>(100000);
    private static ArrayBlockingQueue <MyEntry<String ,Document>> fetchedData = new ArrayBlockingQueue<>(100000);

//    final static String urlTopic = "newUrl5";
//    final static String forParseDataTopic = "new";
    private static Logger logger = LoggerFactory.getLogger(Crawler.class);


    public static void main(String args[]) throws InterruptedException {
//        queue = new Queue();
//        utils.Statistics.getInstance().setThreadsNums(threadNumber, threadNumber);
//        storage = new HBaseSample();
//        storage = new HBase();
        elasticEngine = new Elastic();
//        Queue queue = new Queue();
        Elastic elasticEngine = new Elastic();



        //            **** Q ****
//        System.out.println("seed added");
//        urls.add("https://en.wikipedia.org/wiki/Main_Page");
//        urls.add("https://us.yahoo.com/");
//        urls.add("https://www.nytimes.com/");
//        urls.add("https://www.msn.com/en-us/news");
//        urls.add("http://www.telegraph.co.uk/news/");
//        Queue.add("http://www.alexa.com",5);
//        Queue.add("http://www.apache.org",6);
//        Queue.add("https://en.wikipedia.org/wiki/Main_Page/World_war_II",7);
//        Queue.add("http://www.news.google.com",8);
//        Queue.add("http://www.independent.co.uk",9);
//        ProducerApp producerApp = new ProducerApp();
        logger.info("Seed added.");
        ProducerApp.getMyInstance().send(Constants.URL_TOPIC,"https://en.wikipedia.org/wiki/Main_Page");
        ProducerApp.getMyInstance().send(Constants.URL_TOPIC,"https://us.yahoo.com/");
        ProducerApp.getMyInstance().send(Constants.URL_TOPIC,"https://www.nytimes.com/");
        ProducerApp.getMyInstance().send(Constants.URL_TOPIC,"https://www.msn.com/en-us/news");
        ProducerApp.getMyInstance().send(Constants.URL_TOPIC,"http://www.telegraph.co.uk/news/");
        ProducerApp.getMyInstance().send(Constants.URL_TOPIC,"http://www.alexa.com");
        ProducerApp.getMyInstance().send(Constants.URL_TOPIC,"http://www.apache.org");
        ProducerApp.getMyInstance().send(Constants.URL_TOPIC,"https://en.wikipedia.org/wiki/Main_Page/World_war_II");
        ProducerApp.getMyInstance().send(Constants.URL_TOPIC,"http://www.news.google.com");
        ProducerApp.getMyInstance().send(Constants.URL_TOPIC,"http://www.independent.co.uk");
//            **** Q ****

        Statistics.getInstance().setThreadsNums(Constants.FETCHER_NUMBER,Constants.PARSER_NUMBER);
        Thread stat = new Thread(Statistics.getInstance());
        for (int i = 0; i < Constants.PARSER_NUMBER; i++) {
            new Thread(new Parser(i)).start();
        }
        for (int i = 0 ; i < Constants.FETCHER_NUMBER; i++){
            new Thread(new Fetcher(i)).start();
//            logger.info("thread {} Started.",i);
        }
//        stat.setPriority(6);
//        Thread.sleep(60000);
//        utils.Statistics.getInstance().logStats();
        stat.start();

        ConsumerApp consumerApp = new ConsumerApp();
        consumerApp.start();


//        for (int i = 0 ; i < threadNumber ; i++){
//            threadList.get(i).joinThread();
//            logger.info("thread {} ended.",i);
//        }

//        consumerApp.stop();

//        System.out.println(time / 1000 + "\n");
    }

//    static String takeUrl() {
//        try {
//            return queue.take();
//        } catch (InterruptedException e) {
//            return takeUrl();
//        }
//        try {
//            return urls.take();
//        } catch (InterruptedException e) {
//            return takeUrl();
//        }
//    }
    static MyEntry<String, Document> takeForParseData() {
        try {
            return fetchedData.take();
        } catch (InterruptedException e) {
            return takeForParseData();
        }
    }
//    static void putUrl(String url){
//        queue.add(Constants.URL_TOPIC,url);
////        urls.add(url);
//    }
    static void putForParseData(MyEntry<String , Document> htmlData){
        fetchedData.add(htmlData);
    }
}