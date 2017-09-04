package crawler;

import crawler.Crawler;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import queue.ProducerApp;
import storage.HBase;
import storage.HBaseSample;
import storage.Storage;
import utils.Constants;
import utils.MyEntry;
import utils.Statistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.*;

public class Parser implements Runnable {
    private int threadNum = 0;
    private Thread thread = new Thread(this);
    private static Logger logger = LoggerFactory.getLogger(Crawler.class);
    private ArrayList<String> failedToCheckInHbase = new ArrayList<>();
    private ProducerApp producerInstance = ProducerApp.getMyInstance();
    private Storage storage;
//    private ProducerApp producerApp = new ProducerApp();
//    private HBase storage = new HBase();
Parser(int threadNum) {
    try {
        storage = new HBase(Constants.HBASE_TABLE_NAME,Constants.HBASE_FAMILY_NAME);
    } catch (IOException e) {
        e.printStackTrace();
    }
//    storage = new HBaseSample(Constants.HBASE_TABLE_NAME,Constants.HBASE_TABLE_NAME);
    this.threadNum = threadNum;
}

    @Override
    public void run() {
        logger.info("parser {} Started.",threadNum);
        while (true){
            Elements elements = new Elements();
            MyEntry<String, Document> forParseData = new MyEntry<>();
            String link;
            Document doc ;
            String title ;
            StringBuilder contentBuilder = new StringBuilder();
            String content ;
            String extractedLink;
            String anchor;
            ArrayList<Map.Entry<String, String>> linksAnchors = new ArrayList<>();
            long parseTime = 0;
            long hbasePutTime  = 0;
            long elasticPutTime = 0;
            long hbaseInquiryTime  = 0;
            long hbaseInquiriesTime  = 0;
            long kafkaPutTime = 0;
            long kafkaPutsTime = 0;
            forParseData = Crawler.takeForParseData();
            parseTime = System.currentTimeMillis();
            link = forParseData.getKey();
            doc = forParseData.getValue();
            title = doc.title();
            elements = doc.select("p");
            for (Element element : elements){
                contentBuilder.append(element.text());
            }
            content = contentBuilder.toString();
            elements = doc.select("a[href]");
            for (Element element : elements){
                MyEntry<String , String> linkAnchor = new MyEntry<>();
                extractedLink = element.attr("abs:href");
                anchor = element.text();
                if (extractedLink == null || extractedLink.isEmpty() || extractedLink.equals(link)){
                    continue;
                }
                Boolean hbaseInquiry;
                hbaseInquiryTime = System.currentTimeMillis();
                try {
                    hbaseInquiry = storage.exists(extractedLink);
                } catch (IOException e) {
                    failedToCheckInHbase.add(extractedLink);
                    logger.error("Thread {} while checking existence in HBase:\n{}", threadNum, e.getStackTrace());
                    continue;
                }
                hbaseInquiryTime = System.currentTimeMillis() - hbaseInquiryTime;
                hbaseInquiriesTime += hbaseInquiryTime;
//                hbaseInquiryTimes.add(hbaseInquiryTime);
                if (!hbaseInquiry){
                    kafkaPutTime = System.currentTimeMillis();
                    producerInstance.send(Constants.URL_TOPIC,extractedLink);
                    kafkaPutTime = System.currentTimeMillis() - kafkaPutTime;
                }
                kafkaPutsTime += kafkaPutTime;
                linkAnchor.setKeyVal(extractedLink, anchor);
                linksAnchors.add(linkAnchor);
            }
            Long avgQPutTime;
            Long avgHBaseInqTime;
            if (elements.size() == 0){
                avgQPutTime = 0L;
                avgHBaseInqTime = 0L;
            }else {
                avgQPutTime = kafkaPutsTime/elements.size();
                avgHBaseInqTime = hbaseInquiriesTime/elements.size();
            }
            logger.info("{} inquiry avg time for queue in {}ms {}",threadNum, avgHBaseInqTime, link);
            Statistics.getInstance().addUrlPutQTime(avgQPutTime,threadNum);
            Statistics.getInstance().addHbaseCheckTime(avgHBaseInqTime,threadNum);
            parseTime = ((System.currentTimeMillis() - parseTime) - hbaseInquiriesTime) - kafkaPutsTime;
            Statistics.getInstance().addParseTime(parseTime,threadNum);
            logger.info("{} parsed link in {}ms {}",threadNum, parseTime, link);
            hbasePutTime = System.currentTimeMillis();
            try {
                storage.addLinks(link,linksAnchors);
            } catch (IOException e) {
                logger.error("Thread {} while adding in HBase:\n{}", threadNum, e.getStackTrace());
            }
            hbasePutTime = System.currentTimeMillis() - hbasePutTime;
            Statistics.getInstance().addHbasePutTime(hbasePutTime,threadNum);
            logger.info("{} data added to hbase in {}ms for {}",threadNum,hbasePutTime,link);
            elasticPutTime = System.currentTimeMillis();
            Crawler.elasticEngine.IndexData(link,content,title,"myindex","mytype");
            elasticPutTime = System.currentTimeMillis() - elasticPutTime;
            Statistics.getInstance().addElasticPutTime(elasticPutTime,threadNum);
            logger.info("{} data added to elastic in {}ms for {}",threadNum,threadNum,elasticPutTime,link);
        }
    }
}