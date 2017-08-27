import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class HBase {

    private final static String urlTableName = "UrlsAnchors";
    private final static String urlFamilyName = "Links";

    private Table table;

    private static Configuration sConfig;
    private static Table sTable;

    public HBase() {
        this(urlTableName);
    }

    public HBase(String tableName) {
        // Instantiating HTable class
        table = createTable(tableName);
    }

    private static Table createTable(String tableName) {
        // Instantiating Configuration class
        Configuration config = HBaseConfiguration.create();

        // Instantiating HTable class
        try {
            return ConnectionFactory.createConnection(config).getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void addLinks(String url, ArrayList<Map.Entry<String, String>> links) {
        add(url, links, table);
    }
    public static void sAddLinks(String url, ArrayList<Map.Entry<String, String>> links) {
        Table table = createTable(urlTableName);
        add(url, links, table);
        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static void add(String rowKey, ArrayList<Map.Entry<String, String>> links, Table table) {
//        boolean flag = false;
        // Instantiating Put class
        // accepts a row name.
        Put put = new Put(Bytes.toBytes(rowKey));
        // adding values using addColumn() method
        // accepts column family name, qualifier/row name ,value
        if (links != null)
            for (Map.Entry<String, String> e : links) {
                put.addColumn(Bytes.toBytes(urlFamilyName),
                        Bytes.toBytes(e.getKey()), Bytes.toBytes(e.getValue()));
    //            flag = true;
            }
        // Saving the put Instance to the HTable.
//        if(flag) {
            try {
                table.put(put);
            } catch (IOException e) {
                e.printStackTrace();
            }
//        }
    }

    public boolean createRow(String url) {
        if(!exists(url, table)){
            add(url, null, table);
            return true;
        }
        return false;
    }
    public static boolean sCreateRow(String url) {
        Table table = createTable(urlTableName);
        boolean result = false;
        if(!exists(url, table)){
            add(url, null, table);
            result = true;
        }
        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
    private static boolean exists(String rowKey, Table table) {
        // Instantiating Get class
        Get get = new Get(Bytes.toBytes(rowKey));

        try {
            return table.exists(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}