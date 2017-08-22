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

    //            **** elastic ****
    static Elastic elasticEngine;
    //            **** elastic ****

    public static void main(String args[]){
        int threadNumber = 256;

        cacheLoader = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS)
                .build(new CacheLoader<String, Boolean>() {
                    @Override
                    public Boolean load(String key) throws Exception {
                        return Boolean.FALSE;
                    }
                });
        Queue queue = new Queue(threadNumber);
        elasticEngine = new Elastic();


        //            **** Q ****
        queue.add("https://en.wikipedia.org/wiki/Main_Page",0);
        queue.add("https://us.yahoo.com/",1);
        queue.add("https://www.nytimes.com/",2);
        queue.add("https://www.msn.com/en-us/news",3);
        queue.add("http://www.telegraph.co.uk/news/",4);
//            **** Q ****

        long time = System.currentTimeMillis();
        ArrayList<ParserThread> threadList = new ArrayList<ParserThread>();
        for (int i = 0 ; i < threadNumber ; i++){
            ParserThread parserThread = new ParserThread(cacheLoader, queue, elasticEngine, i);
            threadList.add(parserThread);
        }
        for (int i = 0 ; i < threadNumber ; i++){
            threadList.get(i).joinThread();
        }
        time = System.currentTimeMillis() - time;
        System.out.println(time);
    }
}