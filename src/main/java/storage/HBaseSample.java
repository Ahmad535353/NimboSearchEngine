package storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;



/**
 * An example of using the {@link BufferedMutator} interface.
 * We can run this class on our host and connect to
 * remote host ( host which hbase master is started on it ) ,
 * then transfer data to hbase .
 */
public class HBaseSample extends Configured implements Tool, Storage {

//    private static Logger logger = LoggerFactory.getLogger(crawler.Crawler.class);
    private TableName tableName = TableName.valueOf("aTest");
    private byte[] familyName = Bytes.toBytes("f1");
    private Connection CONN;
    private BufferedMutator mutator;
    private String mRowKey;
    private Map.Entry<String, String>[] mLinks;
    private Table mTable;

    public HBaseSample(String tableName, String familyName) {
        this.tableName = TableName.valueOf(tableName);
        this.familyName = Bytes.toBytes(familyName);
        /** a callback invoked when an asynchronous write fails. */
        BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
//                    logger.info("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };

        BufferedMutatorParams params = new BufferedMutatorParams(this.tableName)
                .listener(listener);

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "server1"); //176.31.102.177
        config.set("hbase.zookeeper.property.clientPort", "2181"); //2181
        try {
            CONN = ConnectionFactory.createConnection(config);
            mutator = CONN.getBufferedMutator(params);
            mTable = CONN.getTable(this.tableName);
        } catch (IOException e) {
//            logger.error(e.getMessage());
        }
    }

    @Override
    public int run(String[] args) {
        Put put = new Put(Bytes.toBytes(mRowKey));
        if (mLinks.length == 0)
            return 0;

        for (Map.Entry<String, String> e : mLinks) {
            put.addColumn(familyName,
                    Bytes.toBytes(e.getKey()), Bytes.toBytes(e.getValue()));
        }
        try {
            mutator.mutate(put);
            mutator.flush();
        } catch (IOException e) {
//            logger.error(e.getMessage());
        }
        return 0;
    }

    public void addLinks(String rowKey, Map.Entry<String, String>[] links) {
        mRowKey = rowKey;
        mLinks = links;
        try {
            ToolRunner.run(this, null);
        } catch (Exception e) {
//            logger.error(e.getMessage());
        }
    }

    public boolean createRowAndCheck(String rowkey) throws IOException {
        if (!exists(rowkey)) {
            Put p = new Put(Bytes.toBytes(rowkey));
            p.addColumn(familyName, Bytes.toBytes("redundant-column"), Bytes.toBytes(""));
            try {
                mutator.mutate(p);
                mutator.flush();
            } catch (IOException e) {
//                logger.error(e.getMessage());
            }
            return false;
        }
        return true;
    }

    public boolean exists(String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        return mTable.exists(get);
    }

    public void closeConnection() {
        try {
            mTable.close();
            mutator.close();
            CONN.close();
        } catch (IOException e) {
//            logger.error(e.getMessage());
        }
    }
}