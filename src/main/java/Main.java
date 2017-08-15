import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String args[]){

//            **** Cache ****
        LoadingCache<String,Boolean> cacheLoader = CacheBuilder.newBuilder().expireAfterWrite(3, TimeUnit.MINUTES)
                .build(new CacheLoader<String, Boolean>() {
                    @Override
                    public Boolean load(String key) throws Exception {
                        return Boolean.TRUE;
                    }
                });
//            **** Cache ****


//            **** Q ****
        ArrayBlockingQueue<String > queue = new ArrayBlockingQueue<String>(10000);
        queue.add("https://en.wikipedia.org/wiki/Main_Page");
//            **** Q ****


        PrintWriter writer = null;
        int fileName = 0;
        org.jsoup.nodes.Document doc = null;
        Elements elements = null;


        while (queue.size() != 0){
            try {
                String link = queue.take();
                URL url = new URL(link);
                String domain = url.getHost();
                Boolean var = cacheLoader.getIfPresent(domain);
                if (var == null){
                    try {
                        doc = Jsoup.connect(link).get();
                        cacheLoader.get(domain);
                        elements = doc.select("a[href]");
                        writer = new PrintWriter("the-file-name" + fileName + ".txt", "UTF-8");
                        fileName++;
                        for (org.jsoup.nodes.Element element : elements){
                            writer.println(element.attr("title"));
                            writer.println(element.attr("abs:href") + "\n\n");
                            queue.add(element.attr("abs:href"));
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                else {
                    queue.add(link);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }

        }
    }
}
