import com.google.common.collect.Multiset;
import com.google.common.net.InternetDomainName;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ArrayBlockingQueue;

public class Fetcher implements Runnable{
    private int threadNum;
    private Thread thread = new Thread(this);
    private Logger logger = LoggerFactory.getLogger(Crawler.class);

    Fetcher(int threadNum){
        this.threadNum = threadNum;
//        thread = new Thread(this);
//        thread.start();
    }
    @Override
    public void run() {
        logger.info("fetcher {} Started.",threadNum);
        while (true){
            String link = null;
            URL url = null;
            org.jsoup.nodes.Document doc = null;
            MyEntry<String,Document> forParseData = new MyEntry<>();
            long connectTime = 0;
            long qTakeTime = System.currentTimeMillis();
            link = Crawler.takeUrl();
            qTakeTime = System.currentTimeMillis() - qTakeTime;
//            qTakeTimes.add(qTakeTime);
            logger.info("{} took {} from Q in time {}ms",threadNum, link, qTakeTime);
            if (link == null || link.isEmpty()) {
                continue;
            }
            try {
                url = new URL(link);
            } catch (MalformedURLException e) {
                logger.error("{} Url malformed {}",threadNum, link);
                continue;
            }

            String domain = url.getHost();
            try {
                domain = InternetDomainName.from(domain).topPrivateDomain().toString();
            } catch (IllegalArgumentException e) {
                logger.error("{} couldn't extract '{}' domain.",threadNum, url);
                continue;
            } catch (IllegalStateException e) {
                logger.error(e.getMessage());
                continue;
            }
            if (domain == null || domain.isEmpty()) {
                continue;
            }
            Boolean var = LruCache.getIfPresent(domain);
            if (var == null){
                logger.info("{} domain {} is allowed.",threadNum, domain);
                for (int j = 0; j < 2; j++) {
                    if (j == 0){
                        logger.info("{} connecting to (first try) {} ... ",threadNum, link);
                    }else {
                        logger.info("{} connecting to (second try) {} ... ",threadNum, link);
                        LruCache.get(domain);
                    }
                    try {
                        connectTime = System.currentTimeMillis();
                        doc = Jsoup.connect(link)
                                .userAgent("Mozilla/5.0 (X11; Linux x86_64; rv:10.0) Gecko/20100101 Firefox/10.0")
                                .ignoreHttpErrors(true).timeout(1000).get();
                        connectTime = System.currentTimeMillis() - connectTime ;
//                        connectTimes.add(connectTime);
                        LruCache.get(domain);
                    } catch (IOException e) {
                        logger.error("{} timeout reached or connection refused. couldn't connect to {}.",threadNum, link);
                        logger.error(e.getMessage());
                        continue;
                    }
                    LruCache.get(domain);
                    logger.info("{} connected in {}ms to {}",threadNum, connectTime, link);
                    forParseData.setKeyVal(link,doc);
                    Crawler.putForParseData(forParseData);
                    break;
                }
            }else {
                logger.info("{} domain {} is not allowed. Back to queue.Queue",threadNum, domain);
                Crawler.putUrl(link);
            }
            Statistics.getInstance().addFetchTime(connectTime,threadNum);
            Statistics.getInstance().addUrlTakeQTime(qTakeTime,threadNum);
        }
    }
//    void joinThread() {
//        try {
//            thread.join();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
}
