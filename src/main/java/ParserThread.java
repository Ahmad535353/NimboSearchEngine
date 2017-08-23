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

public class ParserThread implements Runnable {
    private Thread thread;
    private LoadingCache<String, Boolean> cacheLoader;
    private Queue queue;
    private Elastic elastic;
    private int threadNumber;
    private HBase storage = new HBase();

    public void run() {
        org.jsoup.nodes.Document doc = null;
        Elements elements = null;

        System.out.println("thread started");
        for (int i = 0; i < 30; ) {
            String link = null;
            try {
                link = queue.take(threadNumber);
                System.out.println("taked from Q");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (int j = 0; j < 2; j++) {
                try {
                    URL url = new URL(link);
                    java.net.URLEncoder.encode(String.valueOf(url), "UTF-8");
                    String domain = url.getHost();
                    try {
                        domain = InternetDomainName.from(domain).topPrivateDomain().toString();
                    } catch (IllegalArgumentException e) {
//                            System.out.println(domain);
                    }
                    Boolean var = cacheLoader.getIfPresent(domain);

//              --------------extracted data-------------------------
                    StringBuilder text = new StringBuilder();
//              --------------extracted data-------------------------

                    if (var == null) {
                        Crawler.UCount.incrementAndGet();
                        cacheLoader.get(domain);
                        try {
                            doc = Jsoup.connect(link)
                                    .userAgent("Mozilla/5.0 (X11; Linux x86_64; rv:10.0) Gecko/20100101 Firefox/10.0")
                                    .ignoreHttpErrors(true).timeout(1000).get();

//              --------------extract text and title and language check-----------------------------
                            elements = doc.select("p");
                            for (org.jsoup.nodes.Element element : elements) {
                                text.append(element.text());
                            }
                            String txt = text.toString();

                            if (!LanguageDetector.IsEnglish(txt, 0.05)) {
                                break;
                            }

                            String title = doc.title();
                            elastic.IndexData(link, txt, title, "myindex", "mytype");
//                          storage.addParsedData(link ,doc.title());

//              --------------extract text-----------------------------

                            //              --------------extract urls-----------------------------
                            elements = doc.select("a[href]");
                            String[] links = new String[elements.size()];
                            for (int k = 0; k < elements.size(); k++){
                                String slink = elements.get(k).attr("abs:href");
                                links[k] = slink;
                                if (!storage.exists(slink))     // check url with HBase
                                    queue.add(slink,threadNumber);
                            }
                            storage.addLinks(link, links);    // put urls in HBase
//              --------------extract urls-----------------------------


//


//              --------------result-----------------------------
//                      Will be deleted soon
                        System.out.println("Thread" + threadNumber + " parsed:"); //ahmad
                        System.out.println(link);
                        System.out.println(domain);
                        System.out.println(i);
//              --------------result-----------------------------

                        ++i; // ONE MORE URL FOR THIS THREAD

                        } catch (IOException e) {
//                                e.printStackTrace();
//                        couldn't connect
                        }

                    } else {
//              --------------LRUCache limit---------------------------
//                      add to kafka
                        queue.add(link, threadNumber);
//              --------------LRUCache limit---------------------------
                    }
                    break;
                } catch (ExecutionException e) {
                    if (j == 1) {
                        queue.add(link, threadNumber);
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

    void joinThread() {
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    ParserThread(LoadingCache cache1, Queue q1, Elastic elastic, int threadNumber) {
        this.cacheLoader = cache1;
        this.queue = q1;
        this.threadNumber = threadNumber;
        this.elastic = elastic;
        thread = new Thread(this);
        thread.start();
    }
}