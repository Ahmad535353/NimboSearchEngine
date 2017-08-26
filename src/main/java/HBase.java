import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.HashMap;

public class HBase {
    private HashMap<String, String> urlsStorage = new HashMap<String, String>();
    private Configuration config;
    private Table table;

    public HBase() {
        this("Ali");
    }

    public HBase(String tableName) {
        // Instantiating Configuration class
        config = HBaseConfiguration.create();

        // Instantiating HTable class
        table = null;
        try {
            table = ConnectionFactory.createConnection(config).getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addLinks(String url, String[] links) {
        boolean flag = false;
        // Instantiating Put class
        // accepts a row name.
        Put put = new Put(Bytes.toBytes(url));
        if(url.length() < 1 || url.length() > 500)
            return;
        // adding values using addColumn() method
        // accepts column family name, qualifier/row name ,value
        for (String link : links) {
            if (link.length() > 500 || link.length() < 1){
                continue;
            }
            put.addColumn(Bytes.toBytes("ali"),
                    Bytes.toBytes(link), Bytes.toBytes("1"));
            flag = true;
        }
        // Saving the put Instance to the HTable.
        if(flag) {
            try {
                table.put(put);
                System.out.println("data inserted");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean exists(String url) {
        if (url.length() < 1 || url.length() > 500){
            return true;        // dont add to kafka
        }

        // Instantiating Get class
        Get g = new Get(Bytes.toBytes(url));

        // Reading the data
        Result result = null;
        try {
            result = table.get(g);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return !result.isEmpty();
    }
}