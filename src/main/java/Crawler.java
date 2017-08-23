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

    public static void main(String args[]){
        SearchUI su=new SearchUI("176.31.102.177",9300,"176.31.183.83",9300);
        cacheLoader = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS)
                .build(new CacheLoader<String, Boolean>() {
                    @Override
                    public Boolean load(String key) throws Exception {
                        return Boolean.FALSE;
                    }
                });
        Queue queue = new Queue(32);
        elasticEngine = new Elastic();
        System.out.println("seed added");

        //            **** Q ****
        queue.add("https://en.wikipedia.org/wiki/Main_Page",0);
        queue.add("https://us.yahoo.com/",1);
        queue.add("https://www.nytimes.com/",2);
        queue.add("https://www.msn.com/en-us/news",3);
        queue.add("http://www.telegraph.co.uk/news/",4);
//            **** Q ****

        long time = System.currentTimeMillis();
        ArrayList<ParserThread> threadList = new ArrayList<ParserThread>();
        for (int i = 0 ; i < 32 ; i++){
            ParserThread parserThread = new ParserThread(cacheLoader, queue, elasticEngine, i);
            threadList.add(parserThread);
        }
        for (int i = 0 ; i < 32 ; i++){
            threadList.get(i).joinThread();
        }
        time = System.currentTimeMillis() - time;
        //System.out.println(time); /ahmad
    }
}