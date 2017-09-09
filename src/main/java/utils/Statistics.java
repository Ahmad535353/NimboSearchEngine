package utils;

import crawler.Crawler;
import crawler.ThreadManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class Statistics implements Runnable{

    public static AtomicInteger examineThreadNum = new AtomicInteger(0);
    public static AtomicInteger fetchThreadNum = new AtomicInteger(0);
    public static AtomicInteger parserThreadNum = new AtomicInteger(0);
    public static AtomicInteger kafkaPutThreadNum = new AtomicInteger(0);
    public static AtomicInteger hbasePutThreadNum = new AtomicInteger(0);
    public static AtomicInteger elasticPutThreadNum = new AtomicInteger(0);

    private static Logger logger = LoggerFactory.getLogger(Crawler.class);
    private Logger statLog = LoggerFactory.getLogger("statLogger");
    private Logger avgStatLogger = LoggerFactory.getLogger("avgStatLogger");
    private Logger periodLogger = LoggerFactory.getLogger("periodLogger");

    private ArrayList<Map<String, Long>> threadsTimes;
    private ConcurrentHashMap<String,Long> newTotal;
    private ConcurrentHashMap<String,Long> oldTotal;
    private ConcurrentHashMap<String,Long> realtimeStatistic;

    private int workerThreadNum;

    private final String EXAMINE_LRU1_REJECT_NUM = "examineLru1RejectNum";
    private final String EXAMINE_LRU1_TOTAL_NUM = "examineLru1TotalNum";
    private final String EXAMINE_HBASE_BATCH_CHECK_TIME = "examineHBaseBatchCheckTime";
    private final String EXAMINE_HBASE_BATCH_CHECK_NUM = "examineHBaseBatchCheckNum";
    private final String FETCHER_LRU2_REJECT_NUM = "fetcherLru2RejectNum";
    private final String FETCHER_LRU2_TOTAL_NUM = "fetcherLru2TotalNum";
    private final String FETCH_TIME = "fetchTime";
    private final String FETCH_NUM = "fetchNum";
    private final String PARSER_TIME = "parserTime";
    private final String PARSER_NUM = "parserNum";
    private final String HBASE_PUT_TIME = "HBasePutTime";
    private final String HBASE_PUT_NUM = "HBasePutNum";
    private final String FOR_KAFKA_HBASE_BATCH_CHECK_TIME = "forKafkaHBaseBatchCheckTime";
    private final String FOR_KAFKA_HBASE_BATCH_CHECK_NUM = "forKafkaHBaseBatchCheckNum";
    private final String KAFKA_PUT_TIME = "parserKafkaPutTime";
    private final String KAFKA_PUT_NUM = "parserKafkaPutNum";
    private final String ELASTIC_PUT_TIME = "elasticPutTime";
    private final String ELASTIC_PUT_NUM = "elasticPutNum";
    private final String RATE = "rate";
    private final int REFRESH_TIME = Constants.STATISTIC_REFRESH_TIME / 1000;

//    private int parserThreadNum;
//    private int fetcherThreadNum;
//    private final String FETCHER_FAILED_LRU_NUM = "fetcherFailedLruNum";
//    private final String FETCHER_FETCH_TIME = "fetcherFetchTime";
//    private final String FETCHER_FETCH_NUM = "fetcherFetchNum";
//    private final String FETCHER_FAILED_TO_FETCH_NUM = "fetcherFailedToFetchNum";
//    private final String FETCHER_PUT_FETCHED_DATA_TIME = "fetcherPutFetchedDataTime";
//    private final String FETCHER_PUT_FETCHED_DATA_NUM = "fetcherPutFetchedDataNum";
//    private final String URL_PUT_Q_TIME = "urlPutQTime";
//    private final String URL_PUT_Q_NUM = "urlPutQNum";
//    private final String DOC_TAKE_TIME = "docTakeTime";
//    private final String DOC_TAKE_NUM = "docTakeNum";
//    private final String DOC_PUT_TIME = "docPutTime";
//    private final String DOC_PUT_NUM = "docPutNum";
//    private final String TIMESTAMP = "timestamp";

    private static Statistics myStat = new Statistics();

    public static Statistics getInstance(){
        return myStat;
    }

    void logStats(){
        newTotal = new ConcurrentHashMap<>();
        long first;
        long second;

        for (int i = 0; i < workerThreadNum; i++) {
            Map<String,Long> threadMap = threadsTimes.get(i);

            first = threadMap.get(EXAMINE_LRU1_REJECT_NUM);
            second = threadMap.get(EXAMINE_LRU1_TOTAL_NUM);
            statLog.info("Worker{} rejected {} links from {}",i,first,second);
            statLog.info("Worker{} reject rate is : {}",i,first/second);
            addToTotal(EXAMINE_LRU1_REJECT_NUM,first);
            addToTotal(EXAMINE_LRU1_TOTAL_NUM,second);

            first = threadMap.get(EXAMINE_HBASE_BATCH_CHECK_TIME);
            second = threadMap.get(EXAMINE_HBASE_BATCH_CHECK_NUM);
            statLog.info("Worker{} examine HBase batch check time is {}, check num is {}",i,first, second);
            statLog.info("Worker{} average check time is : {}",i,first/second);
            addToTotal(EXAMINE_HBASE_BATCH_CHECK_TIME,first);
            addToTotal(EXAMINE_HBASE_BATCH_CHECK_NUM,second);


            first = threadMap.get(FETCH_TIME);
            second = threadMap.get(FETCH_NUM);
            statLog.info("Worker{} fetch time is {} and fetch num {}",i,first,second);
            statLog.info("Worker{} average fetch time is : {}",i,first/second);
            addToTotal(FETCH_TIME,first);
            addToTotal(FETCH_NUM,second);

            first = threadMap.get(PARSER_TIME);
            second = threadMap.get(PARSER_NUM);
            statLog.info("Worker{} parse time is {} and num is {}",i,first,second);
            statLog.info("Worker{} average parse time is : {}\n",i,first/second);
            addToTotal(PARSER_TIME,first);
            addToTotal(PARSER_NUM,second);

            first = threadMap.get(HBASE_PUT_TIME);
            second = threadMap.get(HBASE_PUT_NUM);
            statLog.info("Worker{} HBase put time is {} and num is {}",i,first,second);
            statLog.info("Worker{} average Hbase put time is : {}",i,first/second);
            addToTotal(HBASE_PUT_TIME,first);
            addToTotal(HBASE_PUT_NUM,second);

            first = threadMap.get(FOR_KAFKA_HBASE_BATCH_CHECK_TIME);
            second = threadMap.get(FOR_KAFKA_HBASE_BATCH_CHECK_NUM);
            statLog.info("Worker{} HBase check for kafka time is {} and num is {}",i,first,second);
            statLog.info("Worker{} average Hbase check for kafka time is : {}",i,first/second);
            addToTotal(FOR_KAFKA_HBASE_BATCH_CHECK_TIME,first);
            addToTotal(FOR_KAFKA_HBASE_BATCH_CHECK_NUM,second);

            first = threadMap.get(KAFKA_PUT_TIME);
            second = threadMap.get(KAFKA_PUT_NUM);
            statLog.info("Worker{} put in kafka time is {} and num is {}",i,first,second);
            statLog.info("Worker{} average put in kafka time is : {}",i,first/second);
            addToTotal(KAFKA_PUT_TIME,first);
            addToTotal(KAFKA_PUT_NUM,second);

            first = threadMap.get(ELASTIC_PUT_TIME);
            second = threadMap.get(ELASTIC_PUT_NUM);
            statLog.info("Worker{} elastic put time is {}, take num is {}",i,first, second);
            statLog.info("Worker{} elastic put time is : {}",i,first/second);
            addToTotal(ELASTIC_PUT_TIME,first);
            addToTotal(ELASTIC_PUT_NUM,second);

            first = threadMap.get(FETCHER_LRU2_REJECT_NUM);
            second = threadMap.get(FETCHER_LRU2_TOTAL_NUM);
            statLog.info("Worker{} rejected {} links from {}",i,first, second);
            statLog.info("Worker{} reject rate is : {}",i,first/second);
            addToTotal(FETCHER_LRU2_REJECT_NUM,first);
            addToTotal(FETCHER_LRU2_TOTAL_NUM,second);

//            first = thread.get(FETCHER_FAILED_LRU_NUM);
//            statLog.info("Worker{} url take time is {}, take num is {}",i,first, second);
//            addToTotal(FETCHER_FAILED_LRU_NUM,first);
//
//            first = thread.get(FETCHER_FETCH_TIME);
//            second = thread.get(FETCHER_FETCH_NUM);
//            statLog.info("Worker{} fetch time is {}, fetch num is {}",i,first,second);
//            statLog.info("Worker{} average fetch time is : {}",i,first/second);
//            addToTotal(FETCHER_FETCH_TIME,first);
//            addToTotal(FETCHER_FETCH_NUM,second);
//
//            first = thread.get(FETCHER_FAILED_TO_FETCH_NUM);
//            statLog.info("Worker{} had {} failed connection\n",i,first);
//            addToTotal(FETCHER_FAILED_TO_FETCH_NUM,first);
//
//            first = thread.get(FETCHER_PUT_FETCHED_DATA_TIME);
//            second = thread.get(FETCHER_PUT_FETCHED_DATA_NUM);
//            statLog.info("Worker{} fetch time is {}, fetch num is {}",i,first,second);
//            statLog.info("Worker{} average fetch time is : {}",i,first/second);
//            addToTotal(FETCHER_PUT_FETCHED_DATA_TIME,first);
//            addToTotal(FETCHER_PUT_FETCHED_DATA_NUM,second);

        }

//        first = newTotal.get(FETCH_NUM);
//        second =  newTotal.get(FETCH_TIME);
//        avgStatLogger.info("{} links parsed in {}ms", first,second);
//        avgStatLogger.info("average links/sec is {}", second/first);


//        first = newTotal.get(URL_PUT_Q_NUM);
//        second = newTotal.get(URL_PUT_Q_TIME);
//        avgStatLogger.info("{} links added in Kafka in {}ms", first,second);
//        avgStatLogger.info("average links/sec added in Kafka is {}", second/first);
//
//        first = newTotal.get(FETCHER_HBASE_CHECK_NUM);
//        second = newTotal.get(FETCHER_LRU2_REJECT_NUM);
//        avgStatLogger.info("{} links checked with HBase in {}ms", first,second);
//        avgStatLogger.info("average links/sec checked with HBase is {}", second/first);
//
//        first = newTotal.get(PARSER_HBASE_PUT_NUM);
//        second = newTotal.get(HBASE_PUT_TIME);
//        avgStatLogger.info("{} links added to HBase in {}ms", first,second);
//        avgStatLogger.info("average links/sec added to HBase is {}", second/first);
//
//        first = newTotal.get(PARSER_NUM);
//        second = newTotal.get(PARSER_TIME);
//        avgStatLogger.info("{} links added to Elastic in {}ms", first,second);
//        avgStatLogger.info("average links/sec added to Elastic is {}", second/first);
//
//        first = newTotal.get(FETCHER_FETCH_NUM);
//        second = newTotal.get(FETCHER_FETCH_TIME);
//        avgStatLogger.info("{} links fetched in {}ms", first,second);
//        avgStatLogger.info("average fetch time is {}", second/first);
//
//        first = newTotal.get(ELASTIC_PUT_NUM);
//        second = newTotal.get(ELASTIC_PUT_TIME);
//        avgStatLogger.info("{} links taked from buffer in {}ms", first,second);
//        avgStatLogger.info("average buffer time is {}", second/first);
//
//        first = newTotal.get(FETCHER_FAILED_TO_FETCH_NUM);
//        avgStatLogger.info("failed to connect to {} links\n", first);

        if (oldTotal != null){
            realtimeStatistic = new ConcurrentHashMap<>();

            first = newTotal.get(EXAMINE_LRU1_TOTAL_NUM) - oldTotal.get(EXAMINE_LRU1_TOTAL_NUM);
            second =  newTotal.get(EXAMINE_LRU1_REJECT_NUM) - oldTotal.get(EXAMINE_LRU1_REJECT_NUM);
            realtimeStatistic.put(EXAMINE_LRU1_TOTAL_NUM, first);
            realtimeStatistic.put(EXAMINE_LRU1_REJECT_NUM, second);
            periodLogger.info("rejected {} links from {}", second, first);
            if (first != 0)
                periodLogger.info("\treject rate is = {} ms/link", second/first);

            first = newTotal.get(EXAMINE_HBASE_BATCH_CHECK_NUM) - oldTotal.get(EXAMINE_HBASE_BATCH_CHECK_NUM);
            second = newTotal.get(EXAMINE_HBASE_BATCH_CHECK_TIME) - oldTotal.get(EXAMINE_HBASE_BATCH_CHECK_TIME);
            realtimeStatistic.put(EXAMINE_HBASE_BATCH_CHECK_NUM, first);
            realtimeStatistic.put(EXAMINE_HBASE_BATCH_CHECK_TIME, second);
            periodLogger.info("checked {} links with HBase for examine in {}ms", first,second);
            if (first != 0) {
                periodLogger.info("\taverage HBase check time per link = {} ms/link", second/first);
            }

            first = newTotal.get(FETCHER_LRU2_TOTAL_NUM) - oldTotal.get(FETCHER_LRU2_TOTAL_NUM);
            second = newTotal.get(FETCHER_LRU2_REJECT_NUM) - oldTotal.get(FETCHER_LRU2_REJECT_NUM);
            realtimeStatistic.put(FETCHER_LRU2_TOTAL_NUM, first);
            realtimeStatistic.put(FETCHER_LRU2_REJECT_NUM, second);
            periodLogger.info("rejected {} links from {}", second,first);
            if (first != 0) {
                periodLogger.info("\treject rate is = {} ms/link", second/first);
            }

            first = newTotal.get(FETCH_NUM) - oldTotal.get(FETCH_NUM);
            second =  newTotal.get(FETCH_TIME) - oldTotal.get(FETCH_TIME);
            realtimeStatistic.put(FETCH_NUM, first);
            realtimeStatistic.put(FETCH_TIME, second);
            periodLogger.info(" fetched {} links in {}ms", first,second);
            if (first != 0)
                periodLogger.info("\taverage fetch time per link = {} ms/link", second/first);


            first = newTotal.get(PARSER_NUM) - oldTotal.get(PARSER_NUM);
            second = newTotal.get(PARSER_TIME) - oldTotal.get(PARSER_TIME);
            realtimeStatistic.put(PARSER_NUM, first);
            realtimeStatistic.put(PARSER_TIME, second);
            periodLogger.info("parsed {} links in {}ms", first,second);
            if (first != 0)
                periodLogger.info("\taverage parse time per link = {} ms/link", second/first);

            first = newTotal.get(HBASE_PUT_NUM) - oldTotal.get(HBASE_PUT_NUM);
            second = newTotal.get(HBASE_PUT_TIME) - oldTotal.get(HBASE_PUT_TIME);
            realtimeStatistic.put(HBASE_PUT_NUM, first);
            realtimeStatistic.put(HBASE_PUT_TIME, second);
            periodLogger.info("putted {} links in HBase in {}ms", first,second);
            if (first != 0)
                periodLogger.info("\taverage HBase put time per link = {} ms/link", second/first);

            first = newTotal.get(FOR_KAFKA_HBASE_BATCH_CHECK_NUM) - oldTotal.get(FOR_KAFKA_HBASE_BATCH_CHECK_NUM);
            second = newTotal.get(FOR_KAFKA_HBASE_BATCH_CHECK_TIME) - oldTotal.get(FOR_KAFKA_HBASE_BATCH_CHECK_TIME);
            realtimeStatistic.put(FOR_KAFKA_HBASE_BATCH_CHECK_NUM, first);
            realtimeStatistic.put(FOR_KAFKA_HBASE_BATCH_CHECK_TIME, second);
            periodLogger.info("checked {} links with HBase for kafka in {}ms", first,second);
            if (first != 0)
                periodLogger.info("\taverage check time per link = {} ms/link", second/first);

            first = newTotal.get(KAFKA_PUT_NUM) - oldTotal.get(KAFKA_PUT_NUM);
            second = newTotal.get(KAFKA_PUT_TIME) - oldTotal.get(KAFKA_PUT_TIME);
            realtimeStatistic.put(KAFKA_PUT_NUM, first);
            realtimeStatistic.put(KAFKA_PUT_TIME, second);
            periodLogger.info("putted {} links in kafka in {}ms", first,second);
            if (first != 0)
                periodLogger.info("\taverage kafka put time per link = {} ms/link", second/first);

            first = newTotal.get(ELASTIC_PUT_NUM) - oldTotal.get(ELASTIC_PUT_NUM);
            second = newTotal.get(ELASTIC_PUT_TIME) - oldTotal.get(ELASTIC_PUT_TIME);
            realtimeStatistic.put(ELASTIC_PUT_NUM, first);
            realtimeStatistic.put(ELASTIC_PUT_TIME, second);
            periodLogger.info("putted {} links to elastic in {}ms", first,second);
            if (first != 0)
                periodLogger.info("\taverage elastic put time per link = {} ms/link", second/first);
            Long rate = first/ REFRESH_TIME;

            periodLogger.info("links in wait for examine:     {}    {} Threads(fake)      {} Threads(real)"
                    , ThreadManager.kafkaTookUrlQueue.size(),ThreadManager.examineThreadNum
                    , Statistics.examineThreadNum.get());
            periodLogger.info("links in wait for fetch:       {}    {} Threads(fake)      {} Threads(real)"
                    , ThreadManager.allowedUrls.size(), ThreadManager.fetchThreadNum
                    , Statistics.fetchThreadNum.get());
            periodLogger.info("documents in wait for parse:   {}    {} Threads(fake)      {} Threads(real)"
                    , ThreadManager.fetchedData.size(), ThreadManager.parserThreadNum
                    , Statistics.parserThreadNum.get());
            periodLogger.info("data in wait for HBase:        {}    {} Threads(fake)      {} Threads(real)"
                    , ThreadManager.linksAndAnchorsForHbase.size(), ThreadManager.hbasePutThreadNum
                    , Statistics.hbasePutThreadNum.get());
            periodLogger.info("data in wait for elastic:      {}    {} Threads(fake)      {} Threads(real)"
                    , ThreadManager.linkTitleContentForElastic.size(), ThreadManager.elasticPutThreadNum
                    , Statistics.elasticPutThreadNum.get());
            periodLogger.info("data in wait for kafka:        {}    {} Threads(fake)      {} Threads(real)"
                    , ThreadManager.linksForKafka.size(), ThreadManager.kafkaPutThreadNum
                    , Statistics.kafkaPutThreadNum.get());
            periodLogger.info("\t Crawler Rate is {} links/sec\n",rate);

            realtimeStatistic.put(RATE, rate);
//            first = newTotal.get(FETCHER_FAILED_LRU_NUM) - oldTotal.get(FETCHER_FAILED_LRU_NUM);
//            realtimeStatistic.put(FETCHER_FAILED_LRU_NUM, first);
//            periodLogger.info("Worker: {} links rejected by lru", first);


//            first = newTotal.get(FETCHER_FETCH_NUM) - oldTotal.get(FETCHER_FETCH_NUM);
//            second = newTotal.get(FETCHER_FETCH_TIME) - oldTotal.get(FETCHER_FETCH_TIME);
//            realtimeStatistic.put(FETCHER_FETCH_NUM, first);
//            realtimeStatistic.put(FETCHER_FETCH_TIME, second);
//            periodLogger.info("Worker: {} links fetched in {}ms", first,second);
//            if (first != 0)
//                periodLogger.info("\taverage time per link = {} ms/link", second/first);
//
//
//            first = newTotal.get(FETCHER_FAILED_TO_FETCH_NUM) - oldTotal.get(FETCHER_FAILED_TO_FETCH_NUM);
//            realtimeStatistic.put(FETCHER_FAILED_TO_FETCH_NUM, first);
//            periodLogger.info("Worker: {} links failed to fetch", first);
//
//            first = newTotal.get(FETCHER_PUT_FETCHED_DATA_NUM) - oldTotal.get(FETCHER_PUT_FETCHED_DATA_NUM);
//            second = newTotal.get(FETCHER_PUT_FETCHED_DATA_TIME) - oldTotal.get(FETCHER_PUT_FETCHED_DATA_TIME);
//            realtimeStatistic.put(FETCHER_PUT_FETCHED_DATA_NUM, first);
//            realtimeStatistic.put(FETCHER_PUT_FETCHED_DATA_TIME, second);
//            periodLogger.info("Worker: {} fetchedLinks putted in queue in {}ms", first,second);
//            if (first != 0)
//                periodLogger.info("\taverage time per link = {} ms/link", second/first);
//            sendStatistic();


        }
        oldTotal = newTotal;
    }

    private void addToTotal(String key, Long value){
        if (!newTotal.containsKey(key)){
            newTotal.put(key,value);
        }
        else {
            Long oldVal = newTotal.get(key);
            Long newVal = oldVal+value;
            newTotal.put(key,newVal);
        }
    }

    public void setThreadsNums(int workerThreadNumber){
        this.workerThreadNum = workerThreadNumber;
        threadsTimes = new ArrayList<>();
        for (int i = 0; i < workerThreadNum; i++) {
            ConcurrentHashMap<String,Long> tmp = new ConcurrentHashMap<>();

            tmp.put(EXAMINE_LRU1_REJECT_NUM, 1L);
            tmp.put(EXAMINE_LRU1_TOTAL_NUM, 1L);
            tmp.put(FETCH_TIME, 1L);
            tmp.put(FETCH_NUM, 1L);
            tmp.put(PARSER_TIME, 1L);
            tmp.put(PARSER_NUM, 1L);
            tmp.put(HBASE_PUT_TIME,1L);
            tmp.put(HBASE_PUT_NUM,1L);
            tmp.put(FOR_KAFKA_HBASE_BATCH_CHECK_TIME, 1L);
            tmp.put(FOR_KAFKA_HBASE_BATCH_CHECK_NUM, 1L);
            tmp.put(KAFKA_PUT_TIME, 1L);
            tmp.put(KAFKA_PUT_NUM, 1L);

            tmp.put(ELASTIC_PUT_TIME, 1L);
            tmp.put(ELASTIC_PUT_NUM, 1L);
            tmp.put(EXAMINE_HBASE_BATCH_CHECK_TIME, 1L);
            tmp.put(EXAMINE_HBASE_BATCH_CHECK_NUM, 1L);
            tmp.put(FETCHER_LRU2_REJECT_NUM, 1L);
            tmp.put(FETCHER_LRU2_TOTAL_NUM, 1L);
//            tmp.put(FETCHER_FAILED_LRU_NUM,1L);
//            tmp.put(FETCHER_FETCH_TIME, 1L);
//            tmp.put(FETCHER_FETCH_NUM, 1L);
//            tmp.put(FETCHER_FAILED_TO_FETCH_NUM,1L);
//            tmp.put(FETCHER_PUT_FETCHED_DATA_TIME,1L);
//            tmp.put(FETCHER_PUT_FETCHED_DATA_NUM,1L);
//
//            tmp.put(URL_PUT_Q_TIME, 1L);
//            tmp.put(URL_PUT_Q_NUM, 1L);
//            tmp.put(DOC_TAKE_TIME, 1L);
//            tmp.put(DOC_TAKE_NUM, 1L);
//            tmp.put(DOC_PUT_TIME, 1L);
//            tmp.put(DOC_PUT_NUM, 1L);
            threadsTimes.add(tmp);
        }
    }

    public void addLru1RejectNum(Long rejectNum, Long totalNum, int threadNum){
        Long oldRejectNum = threadsTimes.get(threadNum).get(EXAMINE_LRU1_REJECT_NUM);
        Long newRejectNum = oldRejectNum + rejectNum;
        threadsTimes.get(threadNum).put(EXAMINE_LRU1_REJECT_NUM, newRejectNum);

        Long oldNum = threadsTimes.get(threadNum).get(EXAMINE_LRU1_TOTAL_NUM);
        Long newNum = oldNum + totalNum;
        threadsTimes.get(threadNum).put(EXAMINE_LRU1_TOTAL_NUM, newNum);
    }
    public void addFetcherTime(Long fetchTime, int threadNum) {
        addTime(fetchTime, threadNum, FETCH_TIME, FETCH_NUM);
    }
    public void addParserTime(Long parseTime, int threadNum) {
        addTime(parseTime, threadNum, PARSER_TIME, PARSER_NUM);
    }
    public void addHBasePutTime(Long hBasePutTime, int threadNum) {
        addTime(hBasePutTime, threadNum, HBASE_PUT_TIME, HBASE_PUT_NUM);
    }
    public void addForKafkaHBaseBatchCheckTime(Long hBaseCheckTime, int threadNum) {
        addTime(hBaseCheckTime, threadNum, FOR_KAFKA_HBASE_BATCH_CHECK_TIME, FOR_KAFKA_HBASE_BATCH_CHECK_NUM);
    }
    public void addKafkaPutTime(Long kafkaPutTime, int threadNum) {
        addTime(kafkaPutTime, threadNum, KAFKA_PUT_TIME, KAFKA_PUT_NUM);
    }
    public void addElasticPutTime(Long elasticPutTime, int threadNum) {
        addTime(elasticPutTime, threadNum, ELASTIC_PUT_TIME, ELASTIC_PUT_NUM);
    }
    public void addExamineHBaseBatchCheckTime(long hbaseCheckTIme, int threadNum) {
        addTime(hbaseCheckTIme, threadNum, EXAMINE_HBASE_BATCH_CHECK_TIME, EXAMINE_HBASE_BATCH_CHECK_NUM);
    }
    public void addFetcherLru2RejectNum(Long rejectNum, Long totalNum, int threadNum) {
        Long oldRejectNum = threadsTimes.get(threadNum).get(FETCHER_LRU2_REJECT_NUM);
        Long newRejectNum = oldRejectNum + rejectNum;
        threadsTimes.get(threadNum).put(FETCHER_LRU2_REJECT_NUM, newRejectNum);

        Long oldNum = threadsTimes.get(threadNum).get(FETCHER_LRU2_TOTAL_NUM);
        Long newNum = oldNum + totalNum;
        threadsTimes.get(threadNum).put(FETCHER_LRU2_TOTAL_NUM, newNum);
    }
//    public void addFetcherFailedLru(int threadNum) {
//        addNum(threadNum, FETCHER_FAILED_LRU_NUM);
//    }
//    public void addFetcherFetchTime(Long fetchTime, int threadNum) {
//        addTime(fetchTime, threadNum, FETCHER_FETCH_TIME, FETCHER_FETCH_NUM);
//    }
//    public void addFetcherFailedToFetch(int threadNum){
//        addNum(threadNum, FETCHER_FAILED_TO_FETCH_NUM);
//    }
//    public void addFetcherPutFetchedDataTime(Long putTime, int threadNum){
//        addTime(putTime, threadNum, FETCHER_PUT_FETCHED_DATA_TIME, FETCHER_PUT_FETCHED_DATA_NUM);
//    }


    private void addTime(Long time, int threadNum, String timeName, String numName) {
        Long oldTime = threadsTimes.get(threadNum).get(timeName);
        Long newTime = oldTime + time;
        threadsTimes.get(threadNum).put(timeName, newTime);

        Long oldNum = threadsTimes.get(threadNum).get(numName);
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put(numName, newNum);
    }

    @Override
    public void run() {
        statLog.info("at least it works.");
        while (true){
            try {
                Thread.sleep(Constants.STATISTIC_REFRESH_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.logStats();
        }
    }

    private void sendStatistic() {
        try {
            Socket socket = new Socket(Constants.MONITOR_HOST, Constants.MONITOR_PORT);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            objectOutputStream.writeObject(realtimeStatistic);
            objectOutputStream.flush();
            objectOutputStream.close();
            socket.close();
        } catch (IOException e) {
            logger.error("Statistic while sending Statistic:\n{}", Prints.getPrintStackTrace(e));
            return;
        }

    }
}