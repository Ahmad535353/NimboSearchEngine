package crawler;

import com.google.common.net.InternetDomainName;
import org.apache.kafka.clients.producer.Producer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import queue.ProducerApp;
import storage.HBase;
//import storage.HBaseSample;
import storage.HBaseSample;
import storage.Storage;
import utils.Constants;
import utils.MyEntry;
import utils.Statistics;


import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class Fetcher implements Runnable{
    private int threadNum;
    private Thread thread = new Thread(this);
    private Logger logger = LoggerFactory.getLogger(Crawler.class);
    private Storage storage;
//    private ProducerApp producerApp = new ProducerApp();

    Fetcher(int threadNum){
        this.threadNum = threadNum;
//        try {
//            storage = new HBase(Constants.HBASE_TABLE_NAME,Constants.HBASE_FAMILY_NAME);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        storage = new HBaseSample(Constants.HBASE_TABLE_NAME,Constants.HBASE_FAMILY_NAME);
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
            try {
                link = Crawler.urlQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
                continue;
            }
            qTakeTime = System.currentTimeMillis() - qTakeTime;
            try {
                Boolean hbaseInquiry ;
                hbaseInquiry = storage.exists(link);
                if (hbaseInquiry){
                    continue;
                }
            } catch (IOException e) {
                e.printStackTrace();

            }
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

            Boolean var = LruCache.getInstance().getIfPresent(domain);
            if (var == null){
                Statistics.getInstance().addUrlTakeQTime(qTakeTime,threadNum);
                logger.info("{} domain {} is allowed.",threadNum, domain);
                for (int j = 0; j < 2; j++) {
                    if (j == 0){
                        logger.info("{} connecting to (first try) {} ... ",threadNum, link);
                    }else {
                        logger.info("{} connecting to (second try) {} ... ",threadNum, link);
                        LruCache.getInstance().get(domain);
                    }
                    try {
                        connectTime = System.currentTimeMillis();
                        doc = Jsoup.connect(link)
                                .userAgent("Mozilla/5.0 (X11; Linux x86_64; rv:10.0) Gecko/20100101 Firefox/10.0")
                                .ignoreHttpErrors(true).timeout(1000).get();
                        connectTime = System.currentTimeMillis() - connectTime ;
                        Statistics.getInstance().addFetchTime(connectTime,threadNum);
                        LruCache.getInstance().get(domain);
                    } catch (IOException e) {
                        if (j == 1){
                            Statistics.getInstance().increamentFailedLink(threadNum);
                        }
                        logger.error("{} timeout reached or connection refused. couldn't connect to {}.",threadNum, link);
                        logger.error(e.getMessage());
                        continue;
                    }
                    LruCache.getInstance().get(domain);
                    logger.info("{} connected in {}ms to {}",threadNum, connectTime, link);
                    forParseData.setKeyVal(link,doc);
                    Crawler.putForParseData(forParseData);
                    break;
                }
            }else {
                logger.info("{} domain {} is not allowed. Back to Queue",threadNum, domain);
                ProducerApp.getMyInstance().send(Constants.URL_TOPIC,link);
            }
        }
    }
}
