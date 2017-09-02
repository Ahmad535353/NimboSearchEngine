import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import queue.ConsumerApp;
import queue.Queue;
import utils.Constants;

import java.util.concurrent.ArrayBlockingQueue;

public class Crawler {
//    private final static int threadNumber = 30;

//    public static storage.HBase storage;

    public static Elastic elasticEngine ;

    private static ArrayBlockingQueue <MyEntry<String ,Document>> fetchedData = new ArrayBlockingQueue<>(100000);
    private static ArrayBlockingQueue<String> urls = new ArrayBlockingQueue<String>(100000);

//    final static String forParseDataTopic = "new";
    private static Logger logger = LoggerFactory.getLogger(Crawler.class);


    public static void main(String args[]) throws InterruptedException {

//        Statistics.getInstance().setThreadsNums(threadNumber, threadNumber);

        elasticEngine = new Elastic();
        Queue queue = new Queue();
        Elastic elasticEngine = new Elastic();
        LruCache cacheLoader = new LruCache(Constants.LRU_TIME_LIMIT);



        //            **** Q ****
//        System.out.println("seed added");
//        urls.add("https://en.wikipedia.org/wiki/Main_Page");
//        urls.add("https://us.yahoo.com/");
//        urls.add("https://www.nytimes.com/");
//        urls.add("https://www.msn.com/en-us/news");
//        urls.add("http://www.telegraph.co.uk/news/");
//        queue.Queue.add("http://www.alexa.com",5);
//        queue.Queue.add("http://www.apache.org",6);
//        queue.Queue.add("https://en.wikipedia.org/wiki/Main_Page/World_war_II",7);
//        queue.Queue.add("http://www.news.google.com",8);
//        queue.Queue.add("http://www.independent.co.uk",9);
        logger.info("Seed added.");
        Queue.add(Constants.URL_TOPIC,"https://en.wikipedia.org/wiki/Main_Page");
        Queue.add(Constants.URL_TOPIC,"https://us.yahoo.com/");
        Queue.add(Constants.URL_TOPIC,"https://www.nytimes.com/");
        Queue.add(Constants.URL_TOPIC,"https://www.msn.com/en-us/news");
        Queue.add(Constants.URL_TOPIC,"http://www.telegraph.co.uk/news/");
        Queue.add(Constants.URL_TOPIC,"http://www.alexa.com");
        Queue.add(Constants.URL_TOPIC,"http://www.apache.org");
        Queue.add(Constants.URL_TOPIC,"https://en.wikipedia.org/wiki/Main_Page/World_war_II");
        Queue.add(Constants.URL_TOPIC,"http://www.news.google.com");
        Queue.add(Constants.URL_TOPIC,"http://www.independent.co.uk");
//            **** Q ****

        long time = System.currentTimeMillis();

        for (int i = 0; i < Constants.PARSER_NUMBER; i++) {
            new Thread(new Parser(i)).start();
        }
        for (int i = 0; i < Constants.FETCHER_NUMBER; i++){
            new Thread(new Fetcher(i)).start();
//            logger.info("thread {} Started.",i);
        }
        Statistics.getInstance().setThreadsNums(Constants.PARSER_NUMBER, Constants.FETCHER_NUMBER);
        new Thread(Statistics.getInstance()).start();

        ConsumerApp consumerApp = new ConsumerApp();
        consumerApp.start();


//        for (int i = 0 ; i < threadNumber ; i++){
//            threadList.get(i).joinThread();
//            logger.info("thread {} ended.",i);
//        }
        time = System.currentTimeMillis() - time;

//        consumerApp.stop();

//        System.out.println(time / 1000 + "\n");
    }

    static String takeUrl() {
        try {
            return Queue.take();
        } catch (InterruptedException e) {
            return takeUrl();
        }
//        try {
//            return urls.take();
//        } catch (InterruptedException e) {
//            return takeUrl();
//        }
    }
    static MyEntry<String, Document> takeForParseData() {
        try {
            return fetchedData.take();
        } catch (InterruptedException e) {
            return takeForParseData();
        }
    }
    static void putUrl(String url){
        Queue.add(Constants.URL_TOPIC,url);
//        urls.add(url);
    }
    static void putForParseData(MyEntry<String , Document> htmlData){
        fetchedData.add(htmlData);
    }
}