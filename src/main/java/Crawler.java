import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Crawler {
    //            **** Cache ****
    static LoadingCache<String,Boolean> cacheLoader;
    //            **** Cache ****

    //            **** Q ****
    static ArrayBlockingQueue<String > queue;
    //            **** Q ****

    //            **** elastic ****
    static Elastic elasticEngine;
    //            **** elastic ****

    public static void main(String args[]){

        cacheLoader = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS)
                .build(new CacheLoader<String, Boolean>() {
                    @Override
                    public Boolean load(String key) throws Exception {
                        return Boolean.FALSE;
                    }
                });
//        queue = new ArrayBlockingQueue<String>(50000);
        Queue queue = new Queue();
        elasticEngine = new Elastic();


        //            **** Q ****
        queue.add("https://en.wikipedia.org/wiki/Main_Page");
        queue.add("https://us.yahoo.com/");
        queue.add("https://www.nytimes.com/");
        queue.add("https://www.msn.com/en-us/news");
        queue.add("http://www.telegraph.co.uk/news/");
//            **** Q ****

        long time = System.currentTimeMillis();
        ArrayList<ParserThread> threadList = new ArrayList<ParserThread>();
        for (int i = 0 ; i < 1 ; i++){
            ParserThread parserThread = new ParserThread(cacheLoader, queue, elasticEngine, i);
            threadList.add(parserThread);
        }
        for (int i = 0 ; i < 1 ; i++){
            threadList.get(i).joinThread();
        }
        time = System.currentTimeMillis() - time;
        System.out.println(time);
    }
}