import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Statistics implements Runnable{

    private Logger statLog = LoggerFactory.getLogger("statLogger");
    private ArrayList<Map<String, Long>> threadsTimes;
    private int fetcherThreadNum;
    private int parserThreadNum;
    private static Statistics myStat = null;
    final String FETCHTIME = "fetchTime";
    final String FETCHNUM = "fetchNum";
    final String URLTAKEQTIME = "urlTakeQtime";
    final String URLTAKEQNUM = "urlTakeQNum";
    final String PARSETIME = "parseTime";
    final String PARSENUM = "parseNum";
    final String URLPUTQTIME = "urlPutQTime";
    final String URLPUTQNUM = "urlPutQNum";
    final String DOCTAKETIME = "docTakeTime";
    final String DOCTAKENUM = "docTakeNum";
    final String DOCPUTTIME = "docPutTime";
    final String DOCPUTNUM = "docPutNum";
    final String HBASECHECKTIME = "hBaseCheckTime";
    final String HBASECHECKNUM = "hBaseCheckNum";
    final String ELASTICPUTTIME = "elasticPutTime";
    final String ELASTICPUTNUM = "elasticPutNum";


    public static Statistics getInstance(){
        if (myStat == null){
            myStat = new Statistics();
        }
        return myStat;
    }

    void logStats(){
        for (int i = 0; i < parserThreadNum; i++) {
            statLog.info("test");
            Map<String,Long> thread = threadsTimes.get(i);
            statLog.info("thread{} average parse time is : {}",i,thread.get(PARSETIME)/thread.get(PARSENUM));
            statLog.info("thread{} average url put time is : {}",i,thread.get(URLPUTQTIME)/thread.get(URLPUTQNUM));
            statLog.info("thread{} average doc take time is : {}",i,thread.get(DOCTAKETIME)/thread.get(DOCTAKENUM));
            statLog.info("thread{} average doc put time is : {}",i,thread.get(DOCPUTTIME)/thread.get(DOCPUTNUM));
            statLog.info("thread{} average Hbase check time is : {}",i,thread.get(HBASECHECKTIME)/thread.get(HBASECHECKNUM));
            statLog.info("thread{} average elastic put time is : {}",i,thread.get(ELASTICPUTTIME)/thread.get(ELASTICPUTNUM));
            statLog.info("\n");
        }
        for (int i = 0; i < fetcherThreadNum; i++) {
            Map<String,Long> thread = threadsTimes.get(i);
            statLog.info("thread{} average fetch time is : {}",i,thread.get(FETCHTIME)/thread.get(FETCHNUM));
            statLog.info("thread{} average url take time is : {}",i,thread.get(URLTAKEQTIME)/thread.get(URLTAKEQNUM));
            statLog.info("\n");
        }
    }

    void setThreadsNums(int fetcherThreadNum, int parserThreadNum){
        this.fetcherThreadNum = fetcherThreadNum;
        this.parserThreadNum = parserThreadNum;
        threadsTimes = new ArrayList<>();
        int max = Math.max(fetcherThreadNum,parserThreadNum);
        for (int i = 0; i < max; i++) {
            HashMap<String,Long> tmp = new HashMap<>();
            tmp.put("fetchTime", 0L);
            tmp.put("fetchNum", 0L);
            tmp.put("urlTakeQTime", 0L);
            tmp.put("urlTakeQNum", 0L);
            tmp.put("parseTime", 0L);
            tmp.put("parseNum", 0L);
            tmp.put("urlPutQTime", 0L);
            tmp.put("urlPutQNum", 0L);
            tmp.put("docTakeTime", 0L);
            tmp.put("docTakeNum", 0L);
            tmp.put("docPutTime", 0L);
            tmp.put("docPutNum", 0L);
            tmp.put("hBaseCheckTime", 0L);
            tmp.put("hBaseCheckNum", 0L);
            tmp.put("hBasePutTime",0L);
            tmp.put("hBasePutNum",0L);
            tmp.put("elasticPutTime", 0L);
            tmp.put("elasticPutNum", 0L);

            threadsTimes.add(tmp);
        }
        System.out.println("test");
    }

    public void addFetchTime(Long fetchTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get("fetchTime");
        Long newTime = oldTime + fetchTime;
        threadsTimes.get(threadNum).put("fetchTime", newTime);

        Long oldNum = threadsTimes.get(threadNum).get("fetchNum");
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put("fetchNum", newNum);
    }

    public void addUrlTakeQTime(Long urlTakeQTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get("urlTakeQTime");
        Long newTime = oldTime + urlTakeQTime;
        threadsTimes.get(threadNum).put("urlTakeQTime", newTime);

        Long oldNum = threadsTimes.get(threadNum).get("urlTakeQNum");
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put("urlTakeQNum", newNum);
    }

    public void addParseTime(Long parseTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get("parseTime");
        Long newTime = oldTime + parseTime;
        threadsTimes.get(threadNum).put("parseTime", newTime);

        Long oldNum = threadsTimes.get(threadNum).get("parseNum");
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put("parseNum", newNum);
    }

    public void addUrlPutQTime(Long urlPutQTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get("urlPutQTime");
        Long newTime = oldTime + urlPutQTime;
        threadsTimes.get(threadNum).put("urlPutQTime", newTime);

        Long oldNum = threadsTimes.get(threadNum).get("urlPutQNum");
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put("urlPutQNum", newNum);
    }

    public void addDocTakeTime(Long docTakeTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get("docTakeTime");
        Long newTime = oldTime + docTakeTime;
        threadsTimes.get(threadNum).put("docTakeTime", newTime);

        Long oldNum = threadsTimes.get(threadNum).get("docTakeNum");
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put("docTakeNum", newNum);
    }

    public void addDocPutTime(Long docPutTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get("docPutTime");
        Long newTime = oldTime + docPutTime;
        threadsTimes.get(threadNum).put("docPutTime", newTime);

        Long oldNum = threadsTimes.get(threadNum).get("docPutNum");
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put("docPutNum", newNum);
    }

    public void addHbaseCheckTime(Long hBaseCheckTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get("hBaseCheckTime");
        Long newTime = oldTime + hBaseCheckTime;
        threadsTimes.get(threadNum).put("hBaseCheckTime", newTime);

        Long oldNum = threadsTimes.get(threadNum).get("hBaseCheckNum");
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put("hBaseCheckNum", newNum);
    }

    public void addHbasePutTime(Long hBasePutTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get("hBasePutTime");
        Long newTime = oldTime + hBasePutTime;
        threadsTimes.get(threadNum).put("hBasePutTime", newTime);

        Long oldNum = threadsTimes.get(threadNum).get("hBasePutNum");
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put("hBasePutNum", newNum);
    }

    public void addElasticPutTime(Long elasticPutTime, int threadNum) {
        Long oldTime = threadsTimes.get(threadNum).get("elasticPutTime");
        Long newTime = oldTime + elasticPutTime;
        threadsTimes.get(threadNum).put("elasticPutTime", newTime);

        Long oldNum = threadsTimes.get(threadNum).get("elasticPutNum");
        Long newNum = ++oldNum ;
        threadsTimes.get(threadNum).put("elasticPutNum", newNum);
    }

    @Override
    public void run() {
        statLog.info("at least it works.");
        while (true){
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.logStats();
        }
    }
}