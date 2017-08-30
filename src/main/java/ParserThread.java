import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public class ParserThread implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(Crawler.class);
    private Thread thread;
    private ArrayList<String> failedToCheckInHbase = new ArrayList<>();

    Logger timeLogger = LoggerFactory.getLogger("timesLogger");
//    private HBase storage = new HBase();

    public void run() {
        Elements elements;
        MyEntry<String, Document> forParseData;
        String link;
        Document doc ;
        String title ;
        StringBuilder contentBuilder = new StringBuilder();
        String content ;
        String extractedLink;
        String anchor;
        MyEntry<String , String> linkAnchor = new MyEntry<>();
        ArrayList<Map.Entry<String, String>> linksAnchors = new ArrayList<>();
        while (true){
            long parseTime ;
            long hbasePutTime ;
            long elasticPutTime ;
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
                extractedLink = element.attr("abs:href");
                anchor = element.text();
                if (extractedLink == null || extractedLink.isEmpty() || extractedLink.equals(link)){
                    continue;
                }
                Boolean hbaseInquiry;
                hbaseInquiryTime = System.currentTimeMillis();
                try {
                    hbaseInquiry = Crawler.storage.createRowAndCheck(extractedLink);
                } catch (IOException e) {
                    failedToCheckInHbase.add(extractedLink);
                    continue;
                }
                hbaseInquiryTime = System.currentTimeMillis() - hbaseInquiryTime;
                hbaseInquiriesTime += hbaseInquiryTime;
//                hbaseInquiryTimes.add(hbaseInquiryTime);
                if (!hbaseInquiry){
                    Crawler.putUrl(extractedLink);
                    kafkaPutTime = System.currentTimeMillis();
                    Queue.add(Crawler.urlTopic,extractedLink);
                    kafkaPutTime = System.currentTimeMillis() - kafkaPutTime;
                }
                kafkaPutsTime += kafkaPutTime;
                linkAnchor.setKeyVal(extractedLink, anchor);
                linksAnchors.add(linkAnchor);
            }
            parseTime = ((System.currentTimeMillis() - parseTime) - hbaseInquiriesTime) - kafkaPutsTime;
            logger.info("parsed link in {}ms {}", parseTime, link);
//            parseTimes.add(parseTime);

            hbasePutTime = System.currentTimeMillis();
            Crawler.storage.AddLinks(link,linksAnchors);
            hbasePutTime = System.currentTimeMillis() - hbasePutTime;
            logger.info("data added to hbase in {}ms for {}",hbasePutTime,link);
//            hbasePutTimes.add(hbasePutTime);
            elasticPutTime = System.currentTimeMillis();
            Crawler.elasticEngine.IndexData(link,content,title,"myindex","mytype");
            elasticPutTime = System.currentTimeMillis() - elasticPutTime;
            logger.info("data added to elastic in {}ms for {}",elasticPutTime,link);
//            elasticPutTimes.add(elasticPutTime);
        }
    }

    void joinThread() {
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    ParserThread() {
        thread = new Thread(this);
        thread.start();
    }
}