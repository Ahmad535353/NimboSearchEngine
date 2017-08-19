import com.google.common.cache.LoadingCache;
import com.google.common.net.InternetDomainName;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ExecutionException;

public class ParserThread implements Runnable{
    private Thread thread;
    private LoadingCache<String,Boolean> cacheLoader;
//    private ArrayBlockingQueue<String > queue;
    Queue queue;
    private int threadNum;
    private HBase storage = new HBase();
    public void run() {
        org.jsoup.nodes.Document doc = null;
        Elements elements = null;


        for (int i = 0 ; i < 20;){
            try {
                String link = queue.take();
                URL url = new URL(link);
                java.net.URLEncoder.encode(String.valueOf(url), "UTF-8");
                String domain = url.getHost();
                domain = InternetDomainName.from(domain).topPrivateDomain().toString();
                Boolean var = cacheLoader.getIfPresent(domain);
                if (var == null){
                    cacheLoader.get(domain);
                    try {
                        doc = Jsoup.connect(link).userAgent("Mozilla/5.0 (X11; Linux x86_64; rv:10.0) Gecko/20100101 Firefox/10.0").ignoreHttpErrors(true).get();
                        i++;
                        System.out.println("Thread" + threadNum + " parsed:");
                        System.out.println(link);
                        System.out.println(domain);
                        System.out.println(i);


                        elements = doc.select("a[href]");
                        for (org.jsoup.nodes.Element element : elements){
                            String temp = element.attr("abs:href");
                            if (storage.check(temp)) {
                                // this url is parsed before
                            }
                            else {
                                // this url is new
                                queue.add(temp);
                            }
                        }

                        StringBuilder text = new StringBuilder();
                        elements = doc.select("p");
                        for (org.jsoup.nodes.Element element : elements){
                            text.append(element.text());
                        }
                        String txt = text.toString();
                        storage.addParsedData(link ,doc.title());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                else {
//                    System.out.println("Thread" + threadNum + " didn't connected due to LRUCache to " + link);
                    System.out.println(domain);
                    queue.add(link);
                }
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (MalformedURLException e) {
                e.printStackTrace();
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }

        }
    }
    void joinThread(){
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    ParserThread(LoadingCache cache1, Queue q1, PrintWriter writer, int threadNum){
        this.cacheLoader = cache1;
        this.queue = q1;
        this.threadNum = threadNum;
        thread = new Thread(this);
        thread.start();
    }
}
