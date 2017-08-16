import com.google.common.cache.LoadingCache;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;

public class ParserThread implements Runnable{
    private Thread thread;
    private LoadingCache<String,Boolean> cacheLoader;
    private ArrayBlockingQueue<String > queue;
    public int[] time ;
    PrintWriter writer = null;
    public int threadNum;
    public void run() {
        org.jsoup.nodes.Document doc = null;
        Elements elements = null;


        for (int i = 0 ; i < 20;){
            try {
                String link = queue.take();
                URL url = new URL(link);
                java.net.URLEncoder.encode(String.valueOf(url), "UTF-8");
                String domain = url.getHost();
                Boolean var = cacheLoader.getIfPresent(domain);
                if (var == null){
                    cacheLoader.get(domain);
                    try {
                        doc = Jsoup.connect(link).userAgent("Mozilla/5.0 (X11; Linux x86_64; rv:10.0) Gecko/20100101 Firefox/10.0").ignoreHttpErrors(true).get();
                        i++;
                        writer.println("Thread" + threadNum + " parsed:");
                        System.out.println("Thread" + threadNum + " parsed:");
                        writer.println(link);
                        System.out.println(link);
                        System.out.println(domain);
                        System.out.println(i);
                        elements = doc.select("a[href]");
                        for (org.jsoup.nodes.Element element : elements){
                            queue.add(element.attr("abs:href"));
                        }
                    } catch (IOException e) {
//                        e.printStackTrace();
                    }
                }
                else {
                    System.out.println("Thread" + threadNum + " didn't connected due to LRUCache to " + link);
                    queue.add(link);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (MalformedURLException e) {
                e.printStackTrace();
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }

        }
    }
    public void joinThread(){
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public ParserThread(LoadingCache cache1, ArrayBlockingQueue q1, PrintWriter writer, int threadNum){
        this.cacheLoader = cache1;
        this.queue = q1;
        this.writer = writer;
        this.threadNum = threadNum;
        this.time = time;
        thread = new Thread(this);
        thread.start();
    }
}
