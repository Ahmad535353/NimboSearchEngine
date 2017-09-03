package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Statistics implements Runnable{

    private Logger statLog = LoggerFactory.getLogger("statLogger");
    private Logger avgStatLogger = LoggerFactory.getLogger("avgStatLogger");
    private ArrayList<Map<String, Long>> threadsTimes;
    private ConcurrentHashMap<String,Long> total;
    private ConcurrentHashMap<String,Long> periodic;

    private int fetcherThreadNum;
    private int parserThreadNum;
    private static Statistics myStat = null;
    private final String FAILED_TO_FETCH = "failedToFetch";
    private final String FETCH_TIME = "fetchTime";
    private final String FETCH_NUM = "fetchNum";
    private final String URL_TAKE_Q_TIME = "urlTakeQTime";
    private final String URL_TAKE_Q_NUM = "urlTakeQNum";
    private final String PARSE_TIME = "parseTime";
    private final String PARSE_NUM = "parseNum";
    private final String URL_PUT_Q_TIME = "urlPutQTime";
    private final String URL_PUT_Q_NUM = "urlPutQNum";
    private final String DOC_TAKE_TIME = "docTakeTime";
    private final String DOC_TAKE_NUM = "docTakeNum";
    private final String DOC_PUT_TIME = "docPutTime";
    private final String DOC_PUT_NUM = "docPutNum";
    private final String HBASE_CHECK_TIME = "hBaseCheckTime";
    private final String HBASE_CHECK_NUM = "hBaseCheckNum";
    private final String HBASE_PUT_TIME = "hBasePutTime";
    private final String HBASE_PUT_NUM = "hBasePutNum";
    private final String ELASTIC_PUT_TIME = "elasticPutTime";
    private final String ELASTIC_PUT_NUM = "elasticPutNum";


    public static Statistics getInstance(){
        if (myStat == null){
            myStat = new Statistics();
        }
        return myStat;
    }

    void logStats(){
//        long totalParseTime = 0;
//        long totalParseNum = 0;
//        long urlPutTime = 0;
//        long urlPutNum = 0;
        total = new ConcurrentHashMap<>();
        long first;
        long second;
        for (int i = 0; i < parserThreadNum; i++) {
            statLog.info("Thread{}:",i);
            Map<String,Long> thread = threadsTimes.get(i);
            first = thread.get(PARSE_TIME);
            second = thread.get(PARSE_NUM);
            statLog.info("thread{} average parse time is : {}",i,first/second);
            addToTotal(PARSE_TIME,first);
            addToTotal(PARSE_NUM,second);
            periodic.put(PARSE_TIME,first);
            periodic.put(PARSE_NUM,second);

            first = thread.get(URL_PUT_Q_TIME);
            second = thread.get(URL_PUT_Q_NUM);
            statLog.info("thread{} average url put time is : {}",i,first/second);
            addToTotal(URL_PUT_Q_TIME,first);
            addToTotal(URL_PUT_Q_NUM,second);
            periodic.put(URL_PUT_Q_TIME,first);
            periodic.put(URL_PUT_Q_NUM,second);

            first = thread.get(HBASE_CHECK_TIME);
            second = thread.get(HBASE_CHECK_NUM);
            statLog.info("thread{} average Hbase check time is : {}",i,first/second);
            addToTotal(HBASE_CHECK_TIME,first);
            addToTotal(HBASE_CHECK_NUM,second);
            periodic.put(HBASE_CHECK_TIME,first);
            periodic.put(HBASE_CHECK_NUM,second);

            first = thread.get(HBASE_PUT_TIME);
            second = thread.get(HBASE_PUT_NUM);
            statLog.info("thread{} average Hbase put time is : {}",i,first/second);
            addToTotal(HBASE_PUT_TIME,first);
            addToTotal(HBASE_PUT_NUM,second);
            periodic.put(HBASE_PUT_TIME,first);
            periodic.put(HBASE_PUT_NUM,second);

            first = thread.get(ELASTIC_PUT_TIME);
            second = thread.get(ELASTIC_PUT_NUM);
            statLog.info("thread{} average elastic put time is : {}\n",i,first/second);
            addToTotal(ELASTIC_PUT_TIME,first);
            addToTotal(ELASTIC_PUT_NUM,second);
            periodic.put(ELASTIC_PUT_TIME,first);
            periodic.put(ELASTIC_PUT_NUM,second);
        }
        for (int i = 0; i < fetcherThreadNum; i++) {
            Map<String,Long> thread = threadsTimes.get(i);
            first = thread.get(FETCH_TIME);
            second = thread.get(FETCH_NUM);
            statLog.info("thread{} fetch time is {}, fetch num is {}",i,first,second);
            statLog.info("thread{} average fetch time is : {}",i,first/second);
            addToTotal(FETCH_TIME,first);
            addToTotal(FETCH_NUM,second);
            periodic.put(FETCH_TIME,first);
            periodic.put(FETCH_NUM,second);

            first = thread.get(URL_TAKE_Q_TIME);
            second = thread.get(URL_TAKE_Q_NUM);
            statLog.info("thread{} url take time is {}, take num is {}",i,first, second);
            statLog.info("thread{} average url take time is : {}",i,first/second);
            addToTotal(URL_TAKE_Q_TIME,first);
            addToTotal(URL_TAKE_Q_NUM,second);
            periodic.put(URL_TAKE_Q_TIME,first);
            periodic.put(URL_TAKE_Q_NUM,second);

            first = thread.get(FAILED_TO_FETCH);
            statLog.info("thread{} had {} failed connection\n",i,first);
            addToTotal(FAILED_TO_FETCH,first);
            periodic.put(FAILED_TO_FETCH,first);
        }

        first = total.get(PARSE_NUM);
        second =  total.get(PARSE_TIME);
        avgStatLogger.info("{} links parsed in {}ms", first,second);
        avgStatLogger.info("average links/sec is {}", second/first);

        first = total.get(URL_PUT_Q_NUM);
        second = total.get(URL_PUT_Q_TIME);
        avgStatLogger.info("{} links added in Kafka in {}ms", first,second);
        avgStatLogger.info("average links/sec added in Kafka is {}", second/first);

        first = total.get(HBASE_CHECK_NUM);
        second = total.get(HBASE_CHECK_TIME);
        avgStatLogger.info("{} links checked with HBase in {}ms", first,second);
        avgStatLogger.info("average links/sec checked with HBase is {}", second/first);

        first = total.get(HBASE_PUT_NUM);
        second = total.get(HBASE_PUT_TIME);
        avgStatLogger.info("{} links added to HBase in {}ms", first,second);
        avgStatLogger.info("average links/sec added to HBase is {}", second/first);

        first = total.get(ELASTIC_PUT_NUM);
        second = total.get(ELASTIC_PUT_TIME);
        avgStatLogger.info("{} links checked added to Elastic in {}ms", first,second);
        avgStatLogger.info("average links/sec added to Elastic is {}", second/first);

        first = total.get(FETCH_NUM);
        second = total.get(FETCH_TIME);
        avgStatLogger.info("{} links fetched in {}ms", first,second);
        avgStatLogger.info("average fetch time is {}", second/first);

        first = total.get(URL_TAKE_Q_NUM);
        second = total.get(URL_TAKE_Q_TIME);
        avgStatLogger.info("{} links taked from buffer in {}ms", first,second);
        avgStatLogger.info("average buffer time is {}", second/first);

        first = total.get(FAILED_TO_FETCH);
        avgStatLogger.info("failed to connect to {} links\n", first);
    }

    private void addToTotal(String key, Long value){
        if (!total.containsKey(key)){
            total.put(key,value);
        }
        else {
            Long oldVal = total.get(key);
            Long newVal = oldVal+value;
            total.put(key,newVal);
        }
    }
//    private void addToPeriodic(String key, Long value){
//        periodic.put(key,value);
//    }

    public void setThreadsNums(int fetcherThreadNum, int parserThreadNum){
        this.fetcherThreadNum = fetcherThreadNum;
        this.parserThreadNum = parserThreadNum;
        threadsTimes = new ArrayList<>();
        int max = Math.max(fetcherThreadNum,parserThreadNum);
        for (int i = 0; i < max ; i++) {
            ConcurrentHashMap<String,Long> tmp = new ConcurrentHashMap<>();
            tmp.put(FAILED_TO_FETCH,0L);
            tmp.put(FETCH_TIME, 0L);
            tmp.put(FETCH_NUM, 1L);
            tmp.put(URL_TAKE_Q_TIME, 0L);
            tmp.put(URL_TAKE_Q_NUM, 1L);
            tmp.put(PARSE_TIME, 0L);
            tmp.put(PARSE_NUM, 1L);
            tmp.put(URL_PUT_Q_TIME, 0L);
            tmp.put(URL_PUT_Q_NUM, 1L);
            tmp.put(DOC_TAKE_TIME, 0L);
            tmp.put(DOC_TAKE_NUM, 1L);
            tmp.put(DOC_PUT_TIME, 0L);
            tmp.put(DOC_PUT_NUM, 1L);
            tmp.put(HBASE_CHECK_TIME, 0L);
            tmp.put(HBASE_CHECK_NUM, 1L);
            tmp.put(HBASE_PUT_TIME,0L);
            tmp.put(HBASE_PUT_NUM,1L);
            tmp.put(ELASTIC_PUT_TIME, 0L);
            tmp.put(ELASTIC_PUT_NUM, 1L);

            threadsTimes.add(tmp);
        }
        System.out.println("test");
    }

    public void increamentFailedLink(int threadNum){
        Long newNum = threadsTimes.get(threadNum).get(FAILED_TO_FETCH) + 1;
        threadsTimes.get(threadNum).put(FAILED_TO_FETCH, newNum);
    }
    public void addFetchTime(Long fetchTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get(FETCH_TIME);
        Long newTime = oldTime + fetchTime;
        threadsTimes.get(threadNum).put(FETCH_TIME, newTime);

        Long oldNum = threadsTimes.get(threadNum).get(FETCH_NUM);
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put(FETCH_NUM, newNum);
    }

    public void addUrlTakeQTime(Long urlTakeQTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get(URL_TAKE_Q_TIME);
        Long newTime = oldTime + urlTakeQTime;
        threadsTimes.get(threadNum).put(URL_TAKE_Q_TIME, newTime);

        Long oldNum = threadsTimes.get(threadNum).get(URL_TAKE_Q_NUM);
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put(URL_TAKE_Q_NUM, newNum);
    }

    public void addParseTime(Long parseTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get(PARSE_TIME);
        Long newTime = oldTime + parseTime;
        threadsTimes.get(threadNum).put(PARSE_TIME, newTime);

        Long oldNum = threadsTimes.get(threadNum).get(PARSE_NUM);
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put(PARSE_NUM, newNum);
    }

    public void addUrlPutQTime(Long urlPutQTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get(URL_PUT_Q_TIME);
        Long newTime = oldTime + urlPutQTime;
        threadsTimes.get(threadNum).put(URL_PUT_Q_TIME, newTime);

        Long oldNum = threadsTimes.get(threadNum).get(URL_PUT_Q_NUM);
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put(URL_PUT_Q_NUM, newNum);
    }

    public void addDocTakeTime(Long docTakeTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get(DOC_TAKE_TIME);
        Long newTime = oldTime + docTakeTime;
        threadsTimes.get(threadNum).put(DOC_TAKE_TIME, newTime);

        Long oldNum = threadsTimes.get(threadNum).get(DOC_TAKE_NUM);
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put(DOC_TAKE_NUM, newNum);
    }

    public void addDocPutTime(Long docPutTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get(DOC_PUT_TIME);
        Long newTime = oldTime + docPutTime;
        threadsTimes.get(threadNum).put(DOC_PUT_TIME, newTime);

        Long oldNum = threadsTimes.get(threadNum).get(DOC_PUT_NUM);
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put(DOC_PUT_NUM, newNum);
    }

    public void addHbaseCheckTime(Long hBaseCheckTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get(HBASE_CHECK_TIME);
        Long newTime = oldTime + hBaseCheckTime;
        threadsTimes.get(threadNum).put(HBASE_CHECK_TIME, newTime);

        Long oldNum = threadsTimes.get(threadNum).get(HBASE_CHECK_NUM);
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put(HBASE_CHECK_NUM, newNum);
    }

    public void addHbasePutTime(Long hBasePutTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get(HBASE_PUT_TIME);
        Long newTime = oldTime + hBasePutTime;
        threadsTimes.get(threadNum).put(HBASE_PUT_TIME, newTime);

        Long oldNum = threadsTimes.get(threadNum).get(HBASE_PUT_NUM);
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put(HBASE_PUT_NUM, newNum);
    }

    public void addElasticPutTime(Long elasticPutTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get(ELASTIC_PUT_TIME);
        Long newTime = oldTime + elasticPutTime;
        threadsTimes.get(threadNum).put(ELASTIC_PUT_TIME, newTime);

        Long oldNum = threadsTimes.get(threadNum).get(ELASTIC_PUT_NUM);
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put(ELASTIC_PUT_NUM, newNum);
    }

    @Override
    public void run() {
        statLog.info("at least it works.");
        while (true){
            try {
                Thread.sleep(Constants.statisticRefreshTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.logStats();
        }
    }
}