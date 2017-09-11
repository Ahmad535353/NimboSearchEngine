package crawler;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.google.common.net.InternetDomainName;
import elastic.Elastic;
import kafka.ProducerApp;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.HBase;
import utils.Constants;
import utils.Pair;
import utils.Prints;
import utils.Statistics;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class WorkerThread implements Runnable {
    int fixTask;
    private static Elastic elastic = Elastic.getInstance();
    private HBase storage;
    //    private HBaseSample storage;
    private int threadNum;
    private Logger logger = LoggerFactory.getLogger(Crawler.class);
    private LruCache firstCache = Crawler.firstCache;
    private LruCache secondCache = Crawler.secondCache;
    private Statistics statistics = Statistics.getInstance();
    private int taskNumber = 0;
    private Detector detector;

    WorkerThread(int threadNum, int fixTask) {
        this.threadNum = threadNum;
        try {
            storage = new HBase(Constants.HBASE_TABLE_NAME, Constants.HBASE_FAMILY_NAME);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.fixTask = fixTask;
//        try {
//            storage = new HBaseSample(Constants.HBASE_TABLE_NAME, Constants.HBASE_FAMILY_NAME);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        try {
            detector = DetectorFactory.create();
        } catch (LangDetectException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        System.out.println("worker " + threadNum + "started");
        while (true) {
            if (fixTask != 0) {
                taskNumber = fixTask;
            } else {
                taskNumber = decideTask();
            }
            if (taskNumber == 1) {
                Statistics.examineThreadNum.incrementAndGet();
                examineUrl();
                Statistics.examineThreadNum.decrementAndGet();
            } else if (taskNumber == 2) {
                Statistics.fetchThreadNum.incrementAndGet();
                fetchUrl();
                Statistics.fetchThreadNum.decrementAndGet();
            } else if (taskNumber == 3) {
                Statistics.parserThreadNum.incrementAndGet();
                parseDocument();
                Statistics.parserThreadNum.decrementAndGet();
            } else if (taskNumber == 4) {
                Statistics.hbasePutThreadNum.incrementAndGet();
                putInHbase();
                Statistics.hbasePutThreadNum.decrementAndGet();
            } else if (taskNumber == 5) {
                Statistics.elasticPutThreadNum.incrementAndGet();
                putInElastic();
                Statistics.elasticPutThreadNum.decrementAndGet();
            } else if (taskNumber == 6) {
                Statistics.kafkaPutThreadNum.incrementAndGet();
                putInKafka();
                Statistics.kafkaPutThreadNum.decrementAndGet();
            }
        }
    }

    private int decideTask() {
        int[] nums;
        nums = ThreadManager.threadNumArr.clone();
        for (int i = 0; i < nums.length; i++) {
            if (threadNum < nums[i]) {
                return i;
            }
        }
        return 3;
    }

    private void examineUrl() {

//        System.out.println("for examine queue " + ThreadManager.kafkaTookUrlQueue.size());
        ArrayList<String> links = new ArrayList<>();

        String link = null;
        Long rejectCounter = 0L;
        for (int i = 0; i < Constants.EXAMINE_BULK_TAKE_SIZE; i++) {
            try {
//                link = ThreadManager.kafkaTookUrlQueue.take();
                link = ThreadManager.kafkaTookUrlQueue.take();
            } catch (InterruptedException e) {
                logger.error("Fetcher {} couldn't take link from queue\n{}.", threadNum, Prints.getPrintStackTrace(e));
            }
            if (link == null || link.isEmpty()) {
                logger.error("Fetcher {} gets null or empty link from queue\n.", threadNum);
                continue;
            }
            if (link.startsWith("mailto")) {
                continue;
            }
            String domain = null;
            try {
                domain = getDomainIfLruAllowed(link, firstCache);
            } catch (Exception e) {
                logger.error("Fetcher {} couldn't extract domain {}\n{}.", threadNum, link
                        , Prints.getPrintStackTrace(e));
            }
            if (domain == null) {
                rejectCounter++;
                ProducerApp.send(Constants.URL_TOPIC, link);
                continue;
            }
            firstCache.get(domain);
            links.add(link);
        }
        statistics.addLru1RejectNum(rejectCounter, (long) Constants.EXAMINE_BULK_TAKE_SIZE, threadNum);


        long time = System.currentTimeMillis();
        boolean[] result = new boolean[0];
        try {
            result = storage.existsAll(links);
        } catch (IOException e) {
            e.printStackTrace();        //barkhord-e-moghtazi
        }
        time = System.currentTimeMillis() - time;
        statistics.addExamineHBaseBatchCheckTime(time, threadNum);

        for (int i = 0; i < result.length; i++) {
            if (!result[i]) {
                try {
                    ThreadManager.allowedUrls.put(links.get(i));
                } catch (InterruptedException e) {
                    logger.error("Fetcher {} couldn't put {} in allowed urls queue \n{}.", threadNum, link
                            , Prints.getPrintStackTrace(e));
                }
            }
        }
    }

    private void fetchUrl() {

//        System.out.println("for fetch queue " + ThreadManager.allowedUrls.size());
        String link = null;
        try {
            link = ThreadManager.allowedUrls.take();
        } catch (InterruptedException e) {
            logger.error("Fetcher {} couldn't take link from queue\n{}.", threadNum, Prints.getPrintStackTrace(e));
        }

        String domain = null;
        try {
            domain = getDomainIfLruAllowed(link, secondCache);
        } catch (Exception e) {
            logger.error("Fetcher {} couldn't extract domain {}\n{}.", threadNum, link
                    , Prints.getPrintStackTrace(e));
        }
        if (domain == null) {
            ProducerApp.send(Constants.URL_TOPIC, link);
            statistics.addFetcherLru2RejectNum(1L, 1L, threadNum);
            return;
        }
        statistics.addFetcherLru2RejectNum(0L, 1L, threadNum);
        secondCache.get(domain);

        Document document = null;

        logger.info("{} connecting to {} ... ", threadNum, link);
        Long connectTime = System.currentTimeMillis();
        Connection.Response res;
        try {
            res = Jsoup.connect(link)
                    .timeout(Constants.FETCH_TIMEOUT).execute();
        } catch (IOException | IllegalArgumentException e) {
            logger.error("Fetcher {} timeout reached or connection refused. couldn't connect to {}:\n{}"
                    , threadNum, link, Prints.getPrintStackTrace(e));
            return;
        }
        String contentType = res.contentType();
        if (contentType == null) {
            return;
        }
        if (!contentType.startsWith("text/")) {
            logger.error("Unhandled content type. Must be text/* but is {} in {}", contentType, link);
        }
        connectTime = System.currentTimeMillis() - connectTime;
        statistics.addFetcherTime(connectTime, threadNum);
        logger.info("{} connected in {}ms to {}", threadNum, connectTime, link);

        Pair<String, Connection.Response> fetchedData = new Pair<>();
        fetchedData.setKeyVal(link, res);
        try {
            ThreadManager.fetchedData.put(fetchedData);
        } catch (InterruptedException e) {
            logger.error("Fetcher {} while putting fetched data in queue:\n{}", threadNum
                    , Prints.getPrintStackTrace(e));
            return;
        }
    }

    private void parseDocument() {
//        System.out.println("for parse queue " + ThreadManager.fetchedData.size());
        Pair<String, Connection.Response> fetchedData;
        Pair[] linkAnchors;
        String link;
        Document document;
        String title;
        String content;
        try {
            fetchedData = ThreadManager.fetchedData.take();
        } catch (InterruptedException e) {
            logger.error("Parser {} while taking fetched data from queue:\n{}", threadNum
                    , Prints.getPrintStackTrace(e));
            return;
        }

        long time = System.currentTimeMillis();

        link = fetchedData.getKey();
        try {
            document = fetchedData.getValue().parse();
        } catch (IOException | IllegalArgumentException e) {
            logger.error("Worker {} couldn't parse {}:\n{}"
                    , threadNum, link, Prints.getPrintStackTrace(e));
            return;
        }

        title = document.title();
        StringBuilder contentBuilder = new StringBuilder();
        for (Element element : document.select("p")) {
            contentBuilder.append(element.text()).append("\n");
        }
        content = contentBuilder.toString();

        detector.append(content);
        String l = null;
        try {
            l = detector.detect();
            if (!l.equals("en")) {
                return;
            }
        } catch (Exception e) {
            logger.info(Prints.getPrintStackTrace(e));
            return;
        }

        linkAnchors = extractLinkAnchors(document).toArray(new Pair[0]);

        time = System.currentTimeMillis() - time;
        statistics.addParserTime(time, threadNum);

        Pair<String, Pair[]> forHbase = new Pair<>();
        forHbase.setKeyVal(link, linkAnchors);
        try {
            ThreadManager.linksAndAnchorsForHbase.put(forHbase);
        } catch (InterruptedException e) {
            logger.error("Thread {} while adding in HBase queue:\n{}", threadNum, Prints.getPrintStackTrace(e));
        }

        ArrayList<String> forElastic = new ArrayList<>();
        forElastic.add(link);
        forElastic.add(title);
        forElastic.add(content);
        try {
            ThreadManager.linkTitleContentForElastic.put(forElastic);
        } catch (InterruptedException e) {
            logger.error("Thread {} while adding in elastic queue:\n{}", threadNum, Prints.getPrintStackTrace(e));
        }

        try {
            ThreadManager.linksForKafka.put(linkAnchors);
        } catch (InterruptedException e) {
            logger.error("Thread {} while adding in kafka pre-queue:\n{}", threadNum, Prints.getPrintStackTrace(e));
        }
    }

    private void putInHbase() {
//        System.out.println("for hbase queue " + ThreadManager.linksAndAnchorsForHbase.size());
        Pair<String, Pair[]> linkAnchors = new Pair<>();
        try {
            linkAnchors = ThreadManager.linksAndAnchorsForHbase.take();
        } catch (InterruptedException e) {
            logger.error("Parser {} while taking link and anchors from queue:\n{}", threadNum
                    , Prints.getPrintStackTrace(e));
        }

        long time = System.currentTimeMillis();

        try {
            storage.addLinks(linkAnchors.getKey(), linkAnchors.getValue());
        } catch (IOException | NullPointerException e) {
            logger.error("Thread {} while adding in HBase:\n{}", threadNum, Prints.getPrintStackTrace(e));
        }

        time = System.currentTimeMillis() - time;
        statistics.addHBasePutTime(time, threadNum);
        logger.info("Parser {} data added to HBase in {}ms for {}", threadNum, time, linkAnchors.getKey());
    }


    private void putInElastic() {
        BulkRequestBuilder bulkRequest = elastic.getClient().prepareBulk();
        int counter = 0;
        while (true) {
            ArrayList<String> tmp = new ArrayList<String>();
            try {
                tmp = ThreadManager.linkTitleContentForElastic.take();
//                System.out.println("Elastic took data for elastic");
            } catch (InterruptedException e) {
                logger.error("Parser {} while taking link and contents from queue:\n{}", threadNum
                        , Prints.getPrintStackTrace(e));
            }
            XContentBuilder builder = null;
            try {
                builder = jsonBuilder()
                        .startObject()
                        .field("title", tmp.get(1))
                        .field("content", tmp.get(2))
                        .field("prscore", 1.0)
                        .field("anchor1", "")
                        .field("anchor2", "")
                        .field("anchor3", "")
                        .field("anchor4", "")
                        .field("anchor5", "")
                        .endObject();
            } catch (IOException e) {
                e.printStackTrace();
            }
//            System.out.println("Elastic json built");
            bulkRequest.add(elastic.getClient().prepareIndex(Constants.ELASTIC_INDEX_NAME,
                    Constants.ELASTIC_TYPE_NAME, tmp.get(0)).setSource(builder));
            counter++;
//            System.out.println(threadNum + " " + counter);
            if (counter == 1000) {
                long time = System.currentTimeMillis();
                logger.info("Parser {} data added to elastic in {}ms for {}", threadNum, time, tmp.get(0));
                BulkResponse bulkResponse = null;
                try {
                    bulkResponse = bulkRequest.get();
                } catch (ActionRequestValidationException e) {
                    logger.info(Prints.getPrintStackTrace(e));
                }
                time = System.currentTimeMillis() - time;
                statistics.addElasticPutTime(time, threadNum);

                boolean var = false;
                try {
                    var = bulkResponse.hasFailures();
                } catch (NullPointerException e) {
                    logger.info(Prints.getPrintStackTrace(e));
                }
                if (var) {
                    handleFailures(bulkResponse);
                }
                counter = 0;
            }
        }
    }

//    private void putInElastic() {
//        System.out.println("for elastic queue " + ThreadManager.linkTitleContentForElastic.size());
//        ArrayList<String> linkTitleContent = new ArrayList<String>();
//        try {
//            linkTitleContent = ThreadManager.linkTitleContentForElastic.take();
//        } catch (InterruptedException e) {
//            logger.error("Parser {} while taking link and contents from queue:\n{}", threadNum
//                    , Prints.getPrintStackTrace(e));
//        }
//        long time = System.currentTimeMillis();
//
////        Crawler.elasticEngine.IndexData(linkTitleContent.get(0), linkTitleContent.get(1), linkTitleContent.get(2)
////                , Constants.ELASTIC_INDEX_NAME, Constants.ELASTIC_TYPE_NAME);
//
//        time = System.currentTimeMillis() - time;
//        statistics.addElasticPutTime(time, threadNum);
//        logger.info("Parser {} data added to elastic in {}ms for {}", threadNum, time, linkTitleContent.get(0));
//    }

    private void putInKafka() {
//        System.out.println("for kafka queue " + ThreadManager.linksForKafka.size());
        Pair[] linkAnchorsForKafka = new Pair[0];
        try {
            linkAnchorsForKafka = ThreadManager.linksForKafka.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        CheckBulkWithHBase(linkAnchorsForKafka);

        long time = System.currentTimeMillis();
        for (Pair<String, String> linkAnchor : linkAnchorsForKafka) {
            if (linkAnchor != null) {
                ProducerApp.send(Constants.URL_TOPIC, linkAnchor.getKey());
            }
        }
        time = System.currentTimeMillis() - time;
        statistics.addKafkaPutTime(time, threadNum);
    }

    private String getDomainIfLruAllowed(String link, LruCache cache) throws IllegalArgumentException,
            IllegalStateException, MalformedURLException {

        URL url = new URL(link);
        String domain = url.getHost();
        domain = InternetDomainName.from(domain).topPrivateDomain().toString();

        if (domain == null || domain.isEmpty()) {
            throw new IllegalArgumentException("domain is null or empty");
        }

        boolean exist = cache.exist(domain);

        if (exist) {
//            logger.info("Fetcher {} domain {} is not allowed. Back to Queue", threadNum, domain);
            return null;
        } else {
            logger.info("Fetcher {} domain {} is allowed.", threadNum, domain);
            return domain;
        }
    }

    private Set<Pair<String, String>> extractLinkAnchors(Document document) {
        Set<Pair<String, String>> linksAnchors = new HashSet<>();
        for (Element element : document.select("a[href]")) {
            String extractedLink = element.attr("abs:href");
            String anchor = element.text();
            if (anchor == null) {
                anchor = "link";
            }
            if (extractedLink == null || extractedLink.isEmpty() || extractedLink.length() > 512) {
                continue;
            }
            Pair<String, String> linkAnchor = new Pair<>();
            linkAnchor.setKeyVal(extractedLink, anchor);
            linksAnchors.add(linkAnchor);
        }
        return linksAnchors;
    }

    private void CheckBulkWithHBase(Pair[] linkAnchorsForKafka) {
        long time = System.currentTimeMillis();
        try {
            storage.existsAll(linkAnchorsForKafka);
        } catch (IOException e) {
            logger.error("Parser {} couldn't check with HBase\n{}.", threadNum
                    , Prints.getPrintStackTrace(e));
        }
        time = System.currentTimeMillis() - time;
        statistics.addForKafkaHBaseBatchCheckTime(time, threadNum);
    }

    private void handleFailures(BulkResponse br) {
        for (BulkItemResponse i : br) {
            if (i.isFailed()) {
//                    System.out.println(i.getId()+"  : Failed");
            }

        }
    }
}
