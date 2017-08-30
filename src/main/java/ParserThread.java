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
        ArrayBlockingQueue <Long> parseTimes= new ArrayBlockingQueue<>(100);
        ArrayBlockingQueue <Long> hbaseTakeTimes= new ArrayBlockingQueue<>(100);
        ArrayBlockingQueue <Long> hbasePutTimes= new ArrayBlockingQueue<>(100);
        ArrayBlockingQueue <Long> elasticPutTimes= new ArrayBlockingQueue<>(100);
        ArrayBlockingQueue <Long> hbaseInquiryTimes= new ArrayBlockingQueue<>(100);
        while (true){
            long parseTime = 0;
            long hbaseTakeTime = 0;
            long hbasePutTime = 0;
            long elasticPutTime = 0;
            long hbaseInquiryTime = 0;
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
                try {
                    hbaseInquiryTime = System.currentTimeMillis();
                    hbaseInquiry = Crawler.storage.createRowAndCheck(extractedLink);
                    hbaseInquiryTime -= System.currentTimeMillis();
                    hbaseInquiryTimes.add(hbaseInquiryTime);
                } catch (IOException e) {
                    failedToCheckInHbase.add(extractedLink);
                    continue;
                }
                if (!hbaseInquiry){
                    Crawler.putUrl(extractedLink);
//                    Queue.add(Crawler.urlTopic,extractedLink);
                }
                linkAnchor.setKeyVal(extractedLink, anchor);
                linksAnchors.add(linkAnchor);
            }

            Crawler.storage.AddLinks(link,linksAnchors);
            Crawler.elasticEngine.IndexData(link,content,title,"myindex","mytype");

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