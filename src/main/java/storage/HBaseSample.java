package storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;


/**
 * @author amirphl
 */
public class HBaseSample implements Storage {

    private TableName tableName;
    private Table mTable;
    private static Connection CONN;
    private BufferedMutator mutator;
    private Map.Entry<String, String>[] mLinks;
    private byte[] familyName;

    public HBaseSample(String tableName, String familyName) throws IOException {
        this.tableName = TableName.valueOf(tableName);
        this.familyName = Bytes.toBytes(familyName);

        if (CONN == null) {
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "server1"); //176.31.102.177
            config.set("hbase.zookeeper.property.clientPort", "2181"); //2181
            CONN = ConnectionFactory.createConnection(config);
        }

        mutator = CONN.getBufferedMutator(new BufferedMutatorParams(this.tableName));
        mTable = CONN.getTable(this.tableName);
    }

    public void addLinks(String rowKey, Map.Entry<String, String>[] links) throws IOException {
        if (mLinks.length == 0)
            return;

        Put put = new Put(Bytes.toBytes(rowKey));
        for (Map.Entry<String, String> e : mLinks)
            put.addColumn(familyName, Bytes.toBytes(e.getKey()), Bytes.toBytes(e.getValue()));
        mutator.mutate(put);
    }

    public void existsAll(Pair<String, String>[] linkAnchors) throws IOException {
        ArrayList<Get> arrayList = new ArrayList<>();

        for (int i = 0; i < linkAnchors.length; i++)
            arrayList.add(new Get(Bytes.toBytes(linkAnchors[i].getKey())));

        boolean[] result = mTable.existsAll(arrayList);

        for (int i = 0; i < linkAnchors.length; i++)
            if (result[i] == true)
                linkAnchors[i] = null;
    }

    @Override
    public boolean exists(String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        return mTable.exists(get);
    }

    public void closeConnection() throws IOException {
        mTable.close();
        mutator.close();
        CONN.close();
    }
}