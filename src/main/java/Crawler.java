import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

public class Crawler {
    private final static int threadNumber = 30;
    private final static int LruTimeLimit = 30;
    public static HBaseSample storage ;
//    public static HBase storage;

    public static Elastic elasticEngine ;

    private static ArrayBlockingQueue <MyEntry<String ,Document>> fetchedData = new ArrayBlockingQueue<>(100000);
    private static ArrayBlockingQueue<String> urls = new ArrayBlockingQueue<String>(100000);

    final static String urlTopic = "newUrl5";
//    final static String forParseDataTopic = "new";
    private static Logger logger = LoggerFactory.getLogger(Crawler.class);


    public static void main(String args[]) throws InterruptedException {

        Statistics.getInstance().setThreadsNums(threadNumber, threadNumber);
        storage = new HBaseSample();
//        storage = new HBase();
        elasticEngine = new Elastic();
        Queue queue = new Queue();
        Elastic elasticEngine = new Elastic();
        LruCache cacheLoader = new LruCache(LruTimeLimit);



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
        logger.info("Seed added.");
        Queue.add(urlTopic,"https://en.wikipedia.org/wiki/Main_Page");
        Queue.add(urlTopic,"https://us.yahoo.com/");
        Queue.add(urlTopic,"https://www.nytimes.com/");
        Queue.add(urlTopic,"https://www.msn.com/en-us/news");
        Queue.add(urlTopic,"http://www.telegraph.co.uk/news/");
        Queue.add(urlTopic,"http://www.alexa.com");
        Queue.add(urlTopic,"http://www.apache.org");
        Queue.add(urlTopic,"https://en.wikipedia.org/wiki/Main_Page/World_war_II");
        Queue.add(urlTopic,"http://www.news.google.com");
        Queue.add(urlTopic,"http://www.independent.co.uk");
//            **** Q ****

        long time = System.currentTimeMillis();

        for (int i = 0; i < 60; i++) {
            new Thread(new Parser(i)).start();
        }
        for (int i = 0 ; i < 10; i++){
            new Thread(new Fetcher(i)).start();
//            logger.info("thread {} Started.",i);
        }
        Statistics ourStat = new Statistics();
        ourStat.setThreadsNums(60,10);
        new Thread(ourStat).start();

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
        Queue.add(urlTopic,url);
//        urls.add(url);
    }
    static void putForParseData(MyEntry<String , Document> htmlData){
        fetchedData.add(htmlData);
    }
}