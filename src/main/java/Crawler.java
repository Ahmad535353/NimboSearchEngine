import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Crawler {
    public static AtomicInteger UCount=new AtomicInteger(0);
    //            **** Cache ****
    static LoadingCache<String,Boolean> cacheLoader;
    //            **** Cache ****
    //            **** elastic ****
    static Elastic elasticEngine;
    //            **** elastic ****
    public static void main(String args[]) throws InterruptedException {
        int threadNumber = 240;

       // SearchUI su = new SearchUI("176.31.102.177",9300,"176.31.183.83",9300);
        cacheLoader = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS)
                .build(new CacheLoader<String, Boolean>() {
                    @Override
                    public Boolean load(String key) throws Exception {
                        return Boolean.FALSE;
                    }
                });
        Queue queue = new Queue(threadNumber);
        elasticEngine = new Elastic();
        System.out.println("seed added");

        //            **** Q ****
        System.out.println("seed added");
        queue.add("https://en.wikipedia.org/wiki/Main_Page",0);
        queue.add("https://us.yahoo.com/",1);
        queue.add("https://www.nytimes.com/",2);
        queue.add("https://www.msn.com/en-us/news",3);
        queue.add("http://www.telegraph.co.uk/news/",4);
        queue.add("http://www.alexa.com",5);
        queue.add("http://www.apache.org",6);
        queue.add("https://en.wikipedia.org/wiki/Main_Page/World_war_II",7);
        queue.add("http://www.news.google.com",8);
        queue.add("http://www.independent.co.uk",9);
//            **** Q ****

        long time = System.currentTimeMillis();
        ArrayList<ParserThread> threadList = new ArrayList<ParserThread>();
        ArrayList<StoreInQ> storeInQS = new ArrayList<StoreInQ>();

        for (int i = 0 ; i < threadNumber; i++){
            ParserThread parserThread = new ParserThread(cacheLoader, queue, elasticEngine, i);
            threadList.add(parserThread);
        }

        for (int i = 0; i < threadNumber / 10; ++i) {
            StoreInQ storeInQ = new StoreInQ(i);
            storeInQS.add(storeInQ);
            storeInQ.start();
        }


        for (int i = 0 ; i < threadNumber ; i++){
            threadList.get(i).joinThread();
        }

        for (int i = 0; i < threadNumber / 10; ++i) {
            storeInQS.get(i).stop();
        }
        time = System.currentTimeMillis() - time;

        System.out.println(time + "  " + UCount); //result
    }
}