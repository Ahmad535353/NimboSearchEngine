import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * An example of using the {@link BufferedMutator} interface.
 */
public class HBaseSample extends Configured implements Tool {

    private final Log LOG = LogFactory.getLog(HBase.class);
    private final TableName TABLE = TableName.valueOf("dataTable");
    private final byte[] FAMILY = Bytes.toBytes("cf1");
    private final Connection CONN;
    private BufferedMutator mutator;
    private String mUrl;
    private HashSet<String> mLinks;
    private ArrayList<String> mAnchors;
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
    }

    public boolean getTableFromConnection() {
        try {
            mTable = CONN.getTable(TABLE);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public int run(String[] args) throws InterruptedException, ExecutionException, TimeoutException, IOException {
        if (mLinks.size() == 0)
            return 0;
        int j = 0;
        for (String li : mLinks) {
            if (li.length() == 0) {
                j++;
                continue;
            }

            String anchorText = mAnchors.get(j);
            j++;
            Put p = new Put(Bytes.toBytes(mUrl));
            p.addColumn(FAMILY, Bytes.toBytes(li), Bytes.toBytes(anchorText));
            mutator.mutate(p);
        }
        mutator.flush();
//        mutator.close();
        return 0;
    }

    public void add(String url, HashSet<String> links, ArrayList<String> anchors) {
        mUrl = url;
        mLinks = links;
        mAnchors = anchors;
    }

    public boolean exists(String url) {
        Get get = new Get(Bytes.toBytes(url));
        get.addFamily(FAMILY);
        Result r = null;
        try {
            r = mTable.get(get);
        } catch (NullPointerException e1) {
            e1.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}