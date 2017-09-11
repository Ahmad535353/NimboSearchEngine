package utils;

public class Constants {

    public final static int WORKER_THREAD_NUMBER = 100;
    public final static int EXAMINE_BULK_TAKE_SIZE = 100;
    public final static int THREAD_MANAGER_REFRESH_TIME = 2000;

    public final static int EXAMINE_THREAD_NUMBER = 40;
    public final static int FETCHER_THREAD_NUMBER = 100;
    public final static int PARSER_THREAD_NUMBER = 8;
    public final static int HBASE_THREAD_NUMBER = 5;
    public final static int ELASTIC_THREAD_NUMBER = 5;
    public final static int KAFKA_THREAD_NUMBER = 5;


    public final static int LRU_TIME_LIMIT = 30;
    public final static int FETCH_TIMEOUT = 1000;
    public final static int STATISTIC_REFRESH_TIME =  2000;

    public final static String HBASE_TABLE_NAME = "la";
    public final static String HBASE_FAMILY_NAME = "la";
    public final static String URL_TOPIC = "la";

    public final static int KAFKA_SLEEP_TIME = 500;

    public final static String ELASTIC_INDEX_NAME =  "myindex";
    public final static String ELASTIC_TYPE_NAME =  "mytype";

    public final static String MONITOR_HOST =  "server1";
    public final static int MONITOR_PORT =  7788;
}
