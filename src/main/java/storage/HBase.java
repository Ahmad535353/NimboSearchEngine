package storage;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class HBase implements Storage {
    private String tableName;
    private String familyName;

    private static Connection connection;
    private Table table;

    public HBase(String tableName, String familyName) throws IOException {
        this.tableName = tableName;
        this.familyName = familyName;
        if (connection == null) {
            connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
        }
        table = createTable(this.tableName);
    }

    private Table createTable(String tableName) throws IOException {
        return connection.getTable(TableName.valueOf(tableName));
    }

    @Override
    public void addLinks(String url, Map.Entry<String, String>[] links) throws IOException {
        if (links == null || links.length == 0) {
            return;
        }
        Put put = new Put(Bytes.toBytes(url));
        for (Map.Entry<String, String> e : links) {
            put.addColumn(Bytes.toBytes(familyName),
                    Bytes.toBytes(e.getKey()), Bytes.toBytes(e.getValue()));
        }
        table.put(put);
    }

    @Override
    public void existsAll(Pair<String, String>[] linkAnchors) throws IOException {
        ArrayList<Get> arrayList = new ArrayList<>();

        for (int i = 0; i < linkAnchors.length; i++)
            arrayList.add(new Get(Bytes.toBytes(linkAnchors[i].getKey())));

        boolean[] result = table.existsAll(arrayList);

        for (int i = 0; i < linkAnchors.length; i++)
            if (result[i] == true)
                linkAnchors[i] = null;
    }

    public boolean[] existsAll(ArrayList<String > links) throws IOException {
        ArrayList<Get> arrayList = new ArrayList<>();

        for (String link : links){
            arrayList.add(new Get(Bytes.toBytes(link)));
        }
        boolean[] result = table.existsAll(arrayList);
        return result;
    }

    @Override
    public boolean exists(String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        return table.exists(get);
    }

    public void sAddLinks(String url, Map.Entry<String, String>[] links) throws IOException {
        Table temp = table;
        table = createTable(tableName);
        addLinks(url, links);
        table.close();
        table = temp;
    }

    public boolean sExists(String rowKey) throws IOException {
        Table table = createTable(tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        boolean result = table.exists(get);
        table.close();
        return result;
    }
}