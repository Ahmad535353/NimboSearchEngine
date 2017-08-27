import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * An example of using the {@link BufferedMutator} interface.
 * We can run this class on our host and connect to
 * remote host ( host which hbase master is started on it ) ,
 * then transfer data to hbase .
 */
public class HBaseSample extends Configured implements Tool {

    private final Log LOG = LogFactory.getLog(HBase.class);
    private final TableName TABLE = TableName.valueOf("UrlsAnchors");
    private final byte[] FAMILY = Bytes.toBytes("Links");
    private final Connection CONN;
    private BufferedMutator mutator;
    private String mRowKey;
    private ArrayList<Map.Entry<String, String>> mLinks;
    private Table mTable;

    public HBaseSample() throws IOException {
        /** a callback invoked when an asynchronous write fails. */
        BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    LOG.info("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };

        BufferedMutatorParams params = new BufferedMutatorParams(TABLE)
                .listener(listener);

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "server1"); //176.31.102.177
        config.set("hbase.zookeeper.property.clientPort", "2181"); //2181
        CONN = ConnectionFactory.createConnection(config);
        mutator = CONN.getBufferedMutator(params);
        mTable = CONN.getTable(TABLE);
    }

    @Override
    public int run(String[] args) throws InterruptedException, ExecutionException, TimeoutException, IOException {
        Put put = new Put(Bytes.toBytes(mRowKey));
        if (mLinks.size() == 0)
            return 0;

        for (Map.Entry<String, String> e : mLinks) {
            put.addColumn(FAMILY,
                    Bytes.toBytes(e.getKey()), Bytes.toBytes(e.getValue()));
        }
        mutator.mutate(put);
        mutator.flush();
        return 0;
    }

    public void add(String rowKey, ArrayList<Map.Entry<String, String>> links) throws Exception {
        mRowKey = rowKey;
        mLinks = links;
        ToolRunner.run(this, null);
    }

    public boolean createRow(String rowkey) throws IOException {
        if (exists(rowkey)) {
//            Put p = new Put(Bytes.toBytes(rowkey));
//            p.addColumn(FAMILY, Bytes.toBytes("redundant-column"), Bytes.toBytes(""));
//            mutator.mutate(p);
//            mutator.flush();
            return true;
        }
        return false;
    }

    private boolean exists(String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        return mTable.exists(get);
    }

    public void closeConnection() throws IOException {
        mTable.close();
        mutator.close();
        CONN.close();
    }
}