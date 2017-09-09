package crawler;

import org.jsoup.Connection;
import storage.HBase;
import utils.Constants;
import utils.Pair;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

public class ThreadManager implements Runnable{
    public static ArrayBlockingQueue<String> kafkaTookUrlQueue;
    public static int examineThreadNum;
    public static ArrayBlockingQueue<String> allowedUrls;
    public static int fetchThreadNum;
    public static ArrayBlockingQueue<Pair<String, Connection.Response>> fetchedData;
    public static int parserThreadNum;
    public static ArrayBlockingQueue<Pair[]> linksForKafka;
    public static int kafkaPutThreadNum;
    public static ArrayBlockingQueue<Pair<String, Pair[]>> linksAndAnchorsForHbase;
    public static int hbasePutThreadNum;
    public static ArrayBlockingQueue<ArrayList<String>> linkTitleContentForElastic;
    public static int elasticPutThreadNum;

    ThreadManager(){
        kafkaTookUrlQueue = new ArrayBlockingQueue<>(1000);
        allowedUrls = new ArrayBlockingQueue<>(1000);
        fetchedData = new ArrayBlockingQueue<>(1000);
        linksForKafka = new ArrayBlockingQueue<>(1000);
        linksAndAnchorsForHbase = new ArrayBlockingQueue<>(1000);
        linkTitleContentForElastic = new ArrayBlockingQueue<>(1000);

        examineThreadNum = Constants.WORKER_THREAD_NUMBER / 6;
        fetchThreadNum = examineThreadNum;
        parserThreadNum = examineThreadNum;
        kafkaPutThreadNum = examineThreadNum;
        hbasePutThreadNum = examineThreadNum;
        elasticPutThreadNum = examineThreadNum;
    }
    public void manage() {
        parserThreadNum = Constants.PARSER_THREAD_NUMBER;
        double fixKafkaTookUrlQueue = kafkaTookUrlQueue.size();
        double fixAllowedUrls = allowedUrls.size();
//        double fixFetchedData = fetchedData.size();
        double fixLinksForKafka = linksForKafka.size();
        double fixLinksAndAnchorsForHbase = linksAndAnchorsForHbase.size();
        double fixLinkTitleContentForElastic = linkTitleContentForElastic.size();

//        double totalData = fixKafkaTakedUrlQueue + fixAllowedUrls + fixFetchedData + fixLinksForKafka
//                + fixLinksAndAnchorsForHbase + fixLinkTitleContentForElastic;
        double totalData = fixKafkaTookUrlQueue + fixAllowedUrls + fixLinksForKafka
                + fixLinksAndAnchorsForHbase + fixLinkTitleContentForElastic;
        if (totalData == 0){
            totalData = 1;
        }
        examineThreadNum = (int) ((fixKafkaTookUrlQueue / totalData) * Constants.WORKER_THREAD_NUMBER);
        if (examineThreadNum == 0){
            examineThreadNum = 1;
        }
        fetchThreadNum = (int) ((fixAllowedUrls / totalData) * Constants.WORKER_THREAD_NUMBER);
        if (fetchThreadNum == 0){
            fetchThreadNum = 1;
        }
//        parserThreadNum = (int) ((fixFetchedData / totalData) * Constants.WORKER_THREAD_NUMBER);
        kafkaPutThreadNum = (int) ((fixLinksForKafka / totalData) * Constants.WORKER_THREAD_NUMBER);
        if (kafkaPutThreadNum == 0){
            kafkaPutThreadNum = 1;
        }
        hbasePutThreadNum = (int) ((fixLinksAndAnchorsForHbase / totalData) * Constants.WORKER_THREAD_NUMBER);
        if (hbasePutThreadNum == 0){
            hbasePutThreadNum = 1;
        }
        elasticPutThreadNum = (int) ((fixLinkTitleContentForElastic / totalData) * Constants.WORKER_THREAD_NUMBER);
        if (elasticPutThreadNum == 0){
            elasticPutThreadNum = 1;
        }
    }

    @Override
    public void run() {
        while (true){
            try {
                Thread.sleep(Constants.THREAD_MANAGER_REFRESH_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.manage();
        }
    }
}
