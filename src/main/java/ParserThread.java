import com.google.common.cache.LoadingCache;
import com.google.common.net.InternetDomainName;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

public class ParserThread implements Runnable{
    private Thread thread;
    private LoadingCache<String,Boolean> cacheLoader;
    private Queue queue;
    private Elastic elastic;
    private int threadNum;
    private HBase storage = new HBase();
    public void run() {
        org.jsoup.nodes.Document doc = null;
        Elements elements = null;


        for (int i = 0 ; i < 30;i++){
            ArrayList<String> linksRecievedFromKafka = queue.take(threadNum);
            for (int z = 0; z < linksRecievedFromKafka.size(); z++) {
                String link = linksRecievedFromKafka.get(z);
                for (int j = 0; j < 2; j++) {
                    try {
                        URL url = new URL(link);
                        java.net.URLEncoder.encode(String.valueOf(url), "UTF-8");
                        String domain = url.getHost();
                        try{
                            domain = InternetDomainName.from(domain).topPrivateDomain().toString();
                        } catch (IllegalArgumentException e) {
//                            System.out.println("not valid domain ***********************************");
//                            System.out.println(domain);
                        }
                        Boolean var = cacheLoader.getIfPresent(domain);

//              --------------extracted data-------------------------
                        StringBuilder text = new StringBuilder();
//              --------------extracted data-------------------------

                        if (var == null){
                            Crawler.UCount.incrementAndGet();
                            cacheLoader.get(domain);
                            try {
                                doc = Jsoup.connect(link)
                                        .userAgent("Mozilla/5.0 (X11; Linux x86_64; rv:10.0) Gecko/20100101 Firefox/10.0")
                                        .ignoreHttpErrors(true).timeout(250).get();
                                //i++; ahmad
                                String title = doc.title();

                                //System.out.println("Thread" + threadNum + " parsed:"); /ahmad
                                //System.out.println(link); /ahmad
                                //System.out.println(domain); /ahmad
                                //System.out.println(i); /ahmad

//              --------------extract urls-----------------------------
                                elements = doc.select("a[href]");
                                for (org.jsoup.nodes.Element element : elements){
                                    String temp = element.attr("abs:href");
                                    if (storage.check(temp)) {      // check url with HBase
//                                update hbase
                                    }
                                    else {
//                                        add data to Hbase
//                                this url is new. add to kafka
                                        queue.add(temp,threadNum);
                                    }
                                }
//              --------------extract urls-----------------------------

//              --------------extract text-----------------------------
                                elements = doc.select("p");
                                for (org.jsoup.nodes.Element element : elements){
                                    text.append(element.text());
                                }
                                String txt = text.toString();
                                elastic.IndexData(link,txt,title,"myindex","mytype");
//                        storage.addParsedData(link ,doc.title());
                            } catch (IOException e) {
//                                e.printStackTrace();
//                        couldn't connect
                            }
//              --------------extract text-----------------------------
                        }

                        else {
//              --------------LRUCache limit---------------------------
//              add to kafka
//                    System.out.println("Thread" + threadNum + " didn't connected due to LRUCache to " + link);
//                        System.out.println(domain);
                            queue.add(link,threadNum);

//              --------------LRUCache limit---------------------------
                        }
                        break;
                    } catch (ExecutionException e) {
                        if (j == 1){
                            queue.add(link , threadNum);
                        }
//                    e.printStackTrace();
                    } catch (MalformedURLException e) {
//                    e.printStackTrace();
                    } catch (UnsupportedEncodingException e) {
//                    e.printStackTrace();
                    }
                }
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
    ParserThread(LoadingCache cache1, Queue q1, Elastic elastic, int threadNum){
        this.cacheLoader = cache1;
        this.queue = q1;
        this.threadNum = threadNum;
        this.elastic = elastic;
        thread = new Thread(this);
        thread.start();
    }
}