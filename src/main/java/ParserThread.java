import com.google.common.net.InternetDomainName;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.text.Element;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class ParserThread implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(Crawler.class);
    private Thread thread;
    private LruCache cacheLoader;
    private Queue queue;
    private Elastic elastic;
    private int threadNumber;
    private HBase storage = new HBase();

    public void run() {
        org.jsoup.nodes.Document doc = null;
        Elements elements = null;

//        System.out.println("thread started");

        for (int i = 0; i < 30; ) {
            String link = null;
            try {
                link = queue.take(threadNumber);
                if (Crawler.tempStorage.containsKey(link)){
                    continue;
                }

                logger.info("took {} from Q", link);
//                System.out.println("took from Q");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (int j = 0; j < 2; j++) {
                URL url;
                try {
                    url = new URL(link);
                } catch (MalformedURLException e) {
                    logger.error("Url malformed : {}", link);
                    break;
//                    e.printStackTrace();
                }

                String domain = url.getHost();
                try {
                    domain = InternetDomainName.from(domain).topPrivateDomain().toString();
                } catch (IllegalArgumentException e) {
                    logger.warn("couldn't extract '{}' domain.", url);
                    break;
//                            System.out.println(domain);
                } catch (IllegalStateException e){
                    logger.error(e.getMessage());
                    break;
                }
                Boolean var = cacheLoader.getIfPresent(domain);

//              --------------extracted data-------------------------
                StringBuilder text = new StringBuilder();
//              --------------extracted data-------------------------

                if (var == null) {
                    logger.info("domain {} is allowed.",domain);
                    cacheLoader.get(domain);
                    logger.info("connecting to {}...", link);
                    try {
                        doc = Jsoup.connect(link)
                                .userAgent("Mozilla/5.0 (X11; Linux x86_64; rv:10.0) Gecko/20100101 Firefox/10.0")
                                .ignoreHttpErrors(true).timeout(1000).get();
                    } catch (IOException e) {
                        logger.error("couldn't connect {}. try again.", link);
                        continue;
//                            e.printStackTrace();
//                            couldn't connect
                    }

                    logger.info("connected to {}", link);

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

//              --------------extract text-----------------------------

//              --------------extract urls-----------------------------
                    elements = doc.select("a[href]");
                    Set<String> links = new HashSet<String>();


                    for (org.jsoup.nodes.Element element : elements) {
                        String stringLink = element.attr("abs:href");
                        if (stringLink == null || stringLink.isEmpty() || stringLink.equals(link)) {
                            continue;
                        }
                        links.add(stringLink);
                    }
                    for (String s:links){

                        if (!storage.exists(s))     // check url with HBase
//                        if (!Crawler.tempStorage.containsKey(s)) {
                            queue.add(s, threadNumber);
                        }
                    storage.addLinks(link, (String[]) links.toArray());    // put urls in HBase
//                    Crawler.tempStorage.put(link , true);
//              --------------extract urls-----------------------------


//              --------------result-----------------------------
//                        System.out.println("Thread" + threadNumber + " parsed:"); //ahmad
//                        System.out.println(link);
//                        System.out.println(domain);
//                        System.out.println(i);
//              --------------result-----------------------------
                    ++i; // ONE MORE URL FOR THIS THREAD
                } else {
//              --------------LRUCache limit---------------------------
//                      add to kafka
                    logger.info("domain {} is not allowed.",domain);
                    queue.add(link, threadNumber);
//              --------------LRUCache limit---------------------------
                }
                break;
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

    ParserThread(LruCache cache1, Queue q1, Elastic elastic, int threadNumber) {
        this.cacheLoader = cache1;
        this.queue = q1;
        this.threadNumber = threadNumber;
        this.elastic = elastic;
        thread = new Thread(this);
        thread.start();
    }
}