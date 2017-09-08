package crawler;

import com.google.common.net.InternetDomainName;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.ProducerApp;
import storage.HBase;
//import storage.HBaseSample;
import storage.Storage;
import utils.Constants;
import utils.Pair;
import utils.Prints;
import utils.Statistics;


import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class Fetcher implements Runnable {

    private int threadNum;
    private Logger logger = LoggerFactory.getLogger(Crawler.class);
    private Storage storage;

    Fetcher(int threadNum) {
        this.threadNum = threadNum;
        try {
            storage = new HBase(Constants.HBASE_TABLE_NAME, Constants.HBASE_FAMILY_NAME);
        } catch (IOException e) {
            e.printStackTrace();
        }
//        storage = new HBaseSample(Constants.HBASE_TABLE_NAME,Constants.HBASE_FAMILY_NAME);
    }

    @Override
    public void run() {
        logger.info("fetcher {} Started.", threadNum);
        while (true) {
            String link = null;
            try {
                link = takeUrl();
            } catch (InterruptedException e) {
                logger.error("Fetcher {} couldn't take link from queue\n{}.", threadNum, Prints.getPrintStackTrace(e));
                continue;
            }
            if (link == null || link.isEmpty()) {
                logger.error("Fetcher {} gets null or empty link from queue\n.", threadNum);
                continue;
            }

            String domain = null;
            try {
                domain = getDomainIfLruAllowed(link);
            } catch (Exception e) {
                logger.error("Fetcher {} couldn't extract domain {}\n{}.", threadNum, link
                        , Prints.getPrintStackTrace(e));
            }
            if (domain == null) {
                ProducerApp.send(Constants.URL_TOPIC, link);
                continue;
            }

            try {
                if (CheckWithHBase(link)) {
                    continue;
                }
            } catch (IOException e) {
                logger.error("Fetcher {} couldn't check with HBase {}\n{}.", threadNum, link
                        , Prints.getPrintStackTrace(e));
                ProducerApp.send(Constants.URL_TOPIC, link);
                continue;
            }

            LruCache.getInstance().get(domain);

            Document document = null;
            try {
                document = fetch(link);
            } catch (IOException | IllegalArgumentException | IllegalStateException e) {
                Statistics.getInstance().addFetcherFailedToFetch(threadNum);
                logger.error("Fetcher {} timeout reached or connection refused. couldn't connect to {}:\n{}"
                        , threadNum, link, Prints.getPrintStackTrace(e));
                continue;
            }

            Pair<String, Document> fetchedData = new Pair<>();
            fetchedData.setKeyVal(link, document);
            try {
                putFetchedData(fetchedData);
            } catch (InterruptedException e) {
                logger.error("Fetcher {} while putting fetched data in queue:\n{}", threadNum
                        , Prints.getPrintStackTrace(e));
                continue;
            }
        }
    }

    private String takeUrl() throws InterruptedException {
        long time = System.currentTimeMillis();

        String link = Crawler.urlQueue.take();

        time = System.currentTimeMillis() - time;
        Statistics.getInstance().addFetcherTakeUrlTime(time, threadNum);
        logger.info("{} took {} from Q in time {}ms", threadNum, link, time);

        return link;
    }

    private String getDomainIfLruAllowed(String link) throws IllegalArgumentException, IllegalStateException, MalformedURLException {

        long time = System.currentTimeMillis();

        URL url = new URL(link);
        String domain = url.getHost();
        domain = InternetDomainName.from(domain).topPrivateDomain().toString();

        if (domain == null || domain.isEmpty()) {
            throw new IllegalArgumentException("domain is null or empty");
        }

        boolean exist = LruCache.getInstance().exist(domain);

        time = System.currentTimeMillis() - time;
        Statistics.getInstance().addFetcherLruCheckTime(time, threadNum);

        if (exist) {
            logger.info("Fetcher {} domain {} is not allowed. Back to Queue", threadNum, domain);
            Statistics.getInstance().addFetcherFailedLru(threadNum);
            return null;
        } else {
            logger.info("Fetcher {} domain {} is allowed.", threadNum, domain);
            return domain;
        }
    }

    private boolean CheckWithHBase(String link) throws IOException {
        long time = System.currentTimeMillis();

        boolean result = storage.exists(link);

        time = System.currentTimeMillis() - time;
        Statistics.getInstance().addFetcherHBaseCheckTime(time, threadNum);
        return result;
    }

    private Document fetch(String link) throws IOException, IllegalStateException, IllegalArgumentException {
        logger.info("{} connecting to {} ... ", threadNum, link);
        Long connectTime = System.currentTimeMillis();
        Document doc = Jsoup.connect(link)
                .userAgent("Mozilla/5.0 (X11; Linux x86_64; rv:10.0) Gecko/20100101 Firefox/10.0")
                .ignoreHttpErrors(true).timeout(Constants.FETCH_TIMEOUT).get();
        connectTime = System.currentTimeMillis() - connectTime;
        Statistics.getInstance().addFetcherFetchTime(connectTime, threadNum);
        logger.info("{} connected in {}ms to {}", threadNum, connectTime, link);
        return doc;
    }

    private void putFetchedData(Pair<String, Document> forParseData) throws InterruptedException {
        long time = System.currentTimeMillis();

        Crawler.fetchedData.put(forParseData);

        time = System.currentTimeMillis() - time;

        Statistics.getInstance().addFetcherPutFetchedDataTime(time, threadNum);
    }
}
