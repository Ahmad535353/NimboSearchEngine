package crawler;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.ProducerApp;
import storage.HBase;
import storage.Storage;
import utils.Constants;
import utils.Pair;
import utils.Prints;
import utils.Statistics;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static storage.HBase.setOfUrls;

public class Parser implements Runnable {

    private int threadNum = 0;
    private static Logger logger = LoggerFactory.getLogger(Crawler.class);
    private Storage storage;
    public static AtomicInteger counter = new AtomicInteger(0);
    public static FileWriter fileWriter = null;
    public static final Object SYNC_OBJECT = new Object();

    Parser(int threadNum) {
        this.threadNum = threadNum;
        try {
            storage = new HBase(Constants.HBASE_TABLE_NAME, Constants.HBASE_FAMILY_NAME);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (fileWriter == null)
            try {
                fileWriter = new FileWriter(new File("/home/nimbo_search/list-of-languages"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        //    storage = new HBaseSample(Constants.HBASE_TABLE_NAME,Constants.HBASE_TABLE_NAME);
    }

    @Override
    public void run() {
        logger.info("parser {} Started.", threadNum);

        Detector detector = null;

        try {
            detector = DetectorFactory.create();
        } catch (LangDetectException e) {
            e.printStackTrace();
        }
//        LanguageDetector languageDetector = null;
//        try {
//            List<LanguageProfile> lp = new LanguageProfileReader().readAllBuiltIn();
//            languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
//                    .withProfiles(lp)
//                    .build();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        while (true) {
            String link;
            String title;
            String content;
            Document document;
            Pair<String, String>[] linkAnchors;
            Pair<String, Document> fetchedData;

            try {
                fetchedData = takeFetchedData();
            } catch (InterruptedException e) {
                logger.error("Parser {} while taking fetched data from queue:\n{}", threadNum
                        , Prints.getPrintStackTrace(e));
                continue;
            }


            long time = System.currentTimeMillis();

            link = fetchedData.getKey();
            document = fetchedData.getValue();
            title = document.title();

            StringBuilder contentBuilder = new StringBuilder();
            for (Element element : document.select("p")) {
                contentBuilder.append(element.text() + "\n");
            }
            content = contentBuilder.toString();


            detector.append(content);
            String l = null;
            try {
                long timeOfDetection1 = System.currentTimeMillis();
                l = detector.detect();
                if (!l.equals("en")) {
                    long timeOfDetection2 = System.currentTimeMillis();
                    synchronized (SYNC_OBJECT) {
                        fileWriter.write(l + " " + (timeOfDetection2 - timeOfDetection1) + " ms --- ");
                        counter.incrementAndGet();
                        if (counter.get() == 20) {
                            fileWriter.flush();
                            counter = new AtomicInteger(0);
                        }
                    }
                    continue;
                }
            } catch (LangDetectException e) {
                e.printStackTrace();
                continue;
            } catch (NullPointerException e) {
                e.printStackTrace();
                continue;
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }

//            long t = System.currentTimeMillis();
//            TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
//            TextObject textObject = textObjectFactory.forText(content);
//            Optional<LdLocale> lang = languageDetector.detect(textObject);

            linkAnchors = extractLinkAnchors(document).toArray(new Pair[0]);

            time = System.currentTimeMillis() - time;
            Statistics.getInstance().addParserParseTime(time, threadNum);


//            putToElastic(link, title, content);

            try {
                putAnchorsToHBase(link, linkAnchors);
            } catch (IOException e) {
                logger.error("Thread {} while adding in HBase:\n{}", threadNum, Prints.getPrintStackTrace(e));
            }

            CheckWithHBase(linkAnchors);

            putToKafka(linkAnchors);

        }
    }

    private Pair<String, Document> takeFetchedData() throws InterruptedException {
        Pair<String, Document> fetchedData;
        long time = System.currentTimeMillis();
        fetchedData = Crawler.fetchedData.take();
        time = System.currentTimeMillis() - time;
        Statistics.getInstance().addParserTakeFetchedData(time, threadNum);
        return fetchedData;
    }

    private Set<Pair<String, String>> extractLinkAnchors(Document document) {
        Set<Pair<String, String>> linksAnchors = new HashSet<>();
        for (Element element : document.select("a[href]")) {
            String extractedLink = element.attr("abs:href");
            String anchor = element.text();
            if (extractedLink == null || extractedLink.isEmpty()) {
                continue;
            }
            Pair<String, String> linkAnchor = new Pair<>();
            linkAnchor.setKeyVal(extractedLink, anchor);
            linksAnchors.add(linkAnchor);
        }
        return linksAnchors;
//        Pair<String, String>[] result;
//        if(linksAnchors.isEmpty()){
//            result = new Pair[0];
//        } else {
//            Pair<String, String>[] r = (Pair<String, String>[]) linksAnchors.toArray();
//            result = r;
//        }
//        return result;
    }

    private void putToElastic(String link, String title, String content) {
        long time = System.currentTimeMillis();

        Crawler.elasticEngine.IndexData(link, content, title, Constants.ELASTIC_INDEX_NAME
                , Constants.ELASTIC_TYPE_NAME);

        time = System.currentTimeMillis() - time;
        Statistics.getInstance().addParserElasticPutTime(time, threadNum);
        logger.info("Parser {} data added to elastic in {}ms for {}", threadNum, time, link);
    }

    private void putAnchorsToHBase(String link, Pair<String, String>[] linkAnchors) throws IOException {
        long time = System.currentTimeMillis();

        storage.addLinks(link, linkAnchors);

        time = System.currentTimeMillis() - time;
        Statistics.getInstance().addParserHBasePutTime(time, threadNum);
        logger.info("Parser {} data added to HBase in {}ms for {}", threadNum, time, link);
    }

    private void CheckWithHBase(Pair<String, String>[] linkAnchors) {
        long time = System.currentTimeMillis();

//        for (int i = 0; i < linkAnchors.length; i++) {
//            try {
//                if(storage.exists(linkAnchors[i].getKey())) {
//                    linkAnchors[i] = null;
//                }
//            } catch (IOException e) {
//                logger.error("Parser {} couldn't check with HBase {}\n{}.", threadNum, linkAnchors[i].getKey()
//                        , Prints.getPrintStackTrace(e));
//            }
//        }

        try {
            storage.existsAll(linkAnchors);
        } catch (IOException e) {
            logger.error("Parser {} couldn't check with HBase\n{}.", threadNum
                    , Prints.getPrintStackTrace(e));
        }

        time = System.currentTimeMillis() - time;
        Statistics.getInstance().addParserHBaseCheckTime(time, threadNum);
    }

    private void putToKafka(Pair<String, String>[] linkAnchors) {
        long time = System.currentTimeMillis();
        for (Pair<String, String> linkAnchor : linkAnchors) {
            if (linkAnchor != null) {
                ProducerApp.send(Constants.URL_TOPIC, linkAnchor.getKey());
            }
        }
        time = System.currentTimeMillis() - time;
        Statistics.getInstance().addParserKafkaPutTime(time, threadNum);
    }
}