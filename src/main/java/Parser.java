import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.*;

public class Parser implements Runnable {
    private int threadNum = 0;
    private Thread thread = new Thread(this);
    private static Logger logger = LoggerFactory.getLogger(Crawler.class);
    private ArrayList<String> failedToCheckInHbase = new ArrayList<>();
//    private HBase storage = new HBase();
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
            Long avgQPutTime;
            Long avgHBaseInqTime;
            if (elements.size() == 0){
                avgQPutTime = 0L;
                avgHBaseInqTime = 0L;
            }else {
                avgQPutTime = kafkaPutsTime/elements.size();
                avgHBaseInqTime = hbaseInquiriesTime/elements.size();
            }
            Statistics.getInstance().addUrlPutQTime(avgQPutTime,threadNum);
            Statistics.getInstance().addHbaseCheckTime(avgHBaseInqTime,threadNum);
            parseTime = ((System.currentTimeMillis() - parseTime) - hbaseInquiriesTime) - kafkaPutsTime;
            Statistics.getInstance().addParseTime(parseTime,threadNum);
            logger.info("{} parsed link in {}ms {}",threadNum, parseTime, link);
//            parseTimes.add(parseTime);

            hbasePutTime = System.currentTimeMillis();
            Crawler.storage.addLinks(link,linksAnchors);
            hbasePutTime = System.currentTimeMillis() - hbasePutTime;
            Statistics.getInstance().addHbasePutTime(hbasePutTime,threadNum);
            logger.info("{} data added to hbase in {}ms for {}",threadNum,hbasePutTime,link);
//            hbasePutTimes.add(hbasePutTime);
            elasticPutTime = System.currentTimeMillis();
            Crawler.elasticEngine.IndexData(link,content,title,"myindex","mytype");
            elasticPutTime = System.currentTimeMillis() - elasticPutTime;
            Statistics.getInstance().addElasticPutTime(elasticPutTime,threadNum);
            logger.info("{} data added to elastic in {}ms for {}",threadNum,threadNum,elasticPutTime,link);
//            elasticPutTimes.add(elasticPutTime);
        }
    }

//    void joinThread() {
//        try {
//            thread.join();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
    Parser(int threadNum) {
        this.threadNum = threadNum;
//        thread = new Thread(this);
//        thread.start();
    }
}