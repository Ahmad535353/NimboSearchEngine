import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Main {
    //            **** Cache ****
    static LoadingCache<String,Boolean> cacheLoader;
    //            **** Cache ****

    //            **** Q ****
    static ArrayBlockingQueue<String > queue;

    //            **** Q ****

    //            **** writer ****
    static PrintWriter writer = null;
    //            **** writer ****

    public static void main(String args[]){

        cacheLoader = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS)
                .build(new CacheLoader<String, Boolean>() {
                    @Override
                    public Boolean load(String key) throws Exception {
                        return Boolean.FALSE;
                    }
                });
        queue = new ArrayBlockingQueue<String>(50000);
        try {
            writer = new PrintWriter("the-file-name0.txt", "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
//            **** Q ****
        queue.add("https://en.wikipedia.org/wiki/Main_Page");
        queue.add("https://us.yahoo.com/");
        queue.add("https://www.nytimes.com/");
        queue.add("https://www.msn.com/en-us/news");
        queue.add("http://www.telegraph.co.uk/news/");
//            **** Q ****

        long time = System.currentTimeMillis();
        ArrayList<ParserThread> threadList = new ArrayList<ParserThread>();
        for (int i = 0 ; i < 8 ; i++){

            ParserThread parserThread = new ParserThread(cacheLoader, queue, writer, i);
            threadList.add(parserThread);
        }
        for (int i = 0 ; i < 8 ; i++){
            threadList.get(i).joinThread();
        }
        time = System.currentTimeMillis() - time;
        System.out.println(time);

//        int fileName = 0;
//        org.jsoup.nodes.Document doc = null;
//        Elements elements = null;
//
//
//        while (queue.size() != 0){
//            try {
//                String link = queue.take();
//                URL url = new URL(link);
//                String domain = url.getHost();
//                Boolean var = cacheLoader.getIfPresent(domain);
//                if (var == null){
//                    try {
//                        doc = Jsoup.connect(link).get();
//                        cacheLoader.get(domain);
//                        elements = doc.select("a[href]");
//                        writer = new PrintWriter("the-file-name" + fileName + ".txt", "UTF-8");
//                        fileName++;
//                        for (org.jsoup.nodes.Element element : elements){
//                            writer.println(element.attr("title"));
//                            writer.println(element.attr("abs:href") + "\n\n");
//                            queue.add(element.attr("abs:href"));
//                        }
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
//                else {
//                    queue.add(link);
//                }
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (ExecutionException e) {
//                e.printStackTrace();
//            } catch (MalformedURLException e) {
//                e.printStackTrace();
//            }
//
//        }
    }
}
