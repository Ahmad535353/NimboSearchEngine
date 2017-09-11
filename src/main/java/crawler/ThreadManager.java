package crawler;

import org.jsoup.Connection;
import storage.HBase;
import utils.Constants;
import utils.Pair;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

public class ThreadManager implements Runnable{
    public static ArrayBlockingQueue<String> kafkaTookUrlQueue;
    private static int examineThreadNum;
    public static ArrayBlockingQueue<String> allowedUrls;
    private static int fetchThreadNum;
    public static ArrayBlockingQueue<Pair<String, Connection.Response>> fetchedData;
    private static int parserThreadNum;
    public static ArrayBlockingQueue<Pair[]> linksForKafka;
    private static int kafkaPutThreadNum;
    public static ArrayBlockingQueue<Pair<String, Pair[]>> linksAndAnchorsForHbase;
    private static int hbasePutThreadNum;
    public static ArrayBlockingQueue<ArrayList<String>> linkTitleContentForElastic;
    private static int elasticPutThreadNum;
    public static int[] threadNumArr = new int[6];

    ThreadManager(){
        kafkaTookUrlQueue = new ArrayBlockingQueue<>(20000);
        allowedUrls = new ArrayBlockingQueue<>(2000);
        fetchedData = new ArrayBlockingQueue<>(2000);
        linksForKafka = new ArrayBlockingQueue<>(2000);
        linksAndAnchorsForHbase = new ArrayBlockingQueue<>(2000);
        linkTitleContentForElastic = new ArrayBlockingQueue<>(2000);

        examineThreadNum = (Constants.WORKER_THREAD_NUMBER + Constants.PARSER_THREAD_NUMBER) / 6;
        fetchThreadNum = examineThreadNum;
        parserThreadNum = examineThreadNum;
        kafkaPutThreadNum = examineThreadNum;
        hbasePutThreadNum = examineThreadNum;
        elasticPutThreadNum = examineThreadNum;
        for (int i = 0; i < threadNumArr.length; i++) {
            threadNumArr[i] = (i + 1) * examineThreadNum;
        }
    }
    public void manage() {
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
        threadNumArr[0] = examineThreadNum;
        fetchThreadNum = (int) ((fixAllowedUrls / totalData) * Constants.WORKER_THREAD_NUMBER);
//        if (fetchThreadNum == 0){
//            fetchThreadNum = 1;
//        }
        threadNumArr[1] = threadNumArr[0] + fetchThreadNum;
//        parserThreadNum = (int) ((fixFetchedData / totalData) * Constants.WORKER_THREAD_NUMBER);
        parserThreadNum = Constants.PARSER_THREAD_NUMBER;
        threadNumArr[2] = threadNumArr[1] + parserThreadNum;
        kafkaPutThreadNum = (int) ((fixLinksForKafka / totalData) * Constants.WORKER_THREAD_NUMBER);
//        if (kafkaPutThreadNum == 0){
//            kafkaPutThreadNum = 1;
//        }
        threadNumArr[3] = threadNumArr[2] + kafkaPutThreadNum;
        hbasePutThreadNum = (int) ((fixLinksAndAnchorsForHbase / totalData) * Constants.WORKER_THREAD_NUMBER);
//        if (hbasePutThreadNum == 0){
//            hbasePutThreadNum = 1;
//        }
        threadNumArr[4] = threadNumArr[3] + hbasePutThreadNum;
        elasticPutThreadNum = (int) ((fixLinkTitleContentForElastic / totalData) * Constants.WORKER_THREAD_NUMBER);
//        if (elasticPutThreadNum == 0){
//            elasticPutThreadNum = 1;
//        }
        threadNumArr[5] = threadNumArr[4] + elasticPutThreadNum;
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
