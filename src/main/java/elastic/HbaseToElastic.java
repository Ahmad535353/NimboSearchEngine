package elastic;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author amirphl
 */
public class HbaseToElastic {

    private LinkedBlockingQueue<String> list = new LinkedBlockingQueue();
    private List<String[]> arrayList;
    private Table table;
    private final Object SYNC_OBJ = new Object();
    private int numOfThreads;
    private AtomicBoolean atomicBoolean = new AtomicBoolean(true);
    private FileWriter fw;

    public ElasticAdder(String tableName, int numOfThreads, String logPath) throws IOException {
        this.numOfThreads = numOfThreads;
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "server1");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = ConnectionFactory.createConnection(conf);
        table = connection.getTable(TableName.valueOf(tableName));
        fw = new FileWriter(new File(logPath));

        System.out.println("conf created.");

        arrayList = Collections.synchronizedList(new ArrayList<String[]>());

        Thread adderThread = new Thread(new Adder());
        adderThread.start();

        for (int i = 0; i < numOfThreads; i++) {
            Thread t = new Thread(new Downloader(i));
            t.start();
        }
    }

    private class Adder implements Runnable {

        @Override
        public void run() {
            Scan scan = new Scan();
            scan.setCaching(500);
            System.out.println("scan created.");
            ResultScanner scanner = null;
            try {
                scanner = table.getScanner(scan);
                System.out.println("scanner created.");
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("scanner not created.");
            }
            for (Result r : scanner) {
                try {
                    list.put(new String(r.getRow(), "UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private class Downloader implements Runnable {
        private int id;

        public Downloader(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            Elastic elasticEngine = new Elastic();
            while (atomicBoolean.get()) {
                try {
                    String url;
                    while (true) {
                        url = list.poll();
                        if (url == null) {
                            System.err.println("url == null in thread " + id);
                            continue;
                        }
                        break;
                    }
                    System.out.println(url);
                    org.jsoup.nodes.Document doc = Jsoup.connect(url)
                            .userAgent("Mozilla/5.0 (X11; Linux x86_64; rv:10.0) Gecko/20100101 Firefox/10.0")
                            .ignoreHttpErrors(true).timeout(10000).get();

                    String title = doc.title();
                    StringBuilder contentBuilder = new StringBuilder();
                    String content;
                    Elements elements = doc.select("p");
                    for (Element element : elements) {
                        contentBuilder.append(element.text());
                    }
                    content = contentBuilder.toString();
//                arrayList.add(new String[]{url, content, title, "phl", "mytype"});
                    elasticEngine.IndexData(url, content, title, "phl", "mytype");
                } catch (MalformedURLException e) {
                    System.out.println("MalformedURLException happened in thread " + id);
                } catch (IllegalArgumentException e) {
                    System.out.println("IllegalArgumentException happened in thread " + id);
                } catch (java.net.UnknownHostException e) {
                    System.out.println("UnknownHostException happened in thread " + id);
                } catch (IOException e) {
                    System.out.println("IOException happened in thread " + id);
                }
//                catch (Exception e) {
//                    System.out.println("Exception happened.");
//                }
            }
            System.err.println("Thread " + id + " dead. " + atomicBoolean.get());
            try {
                synchronized (SYNC_OBJ) {
                    fw.write("Thread " + id + " dead. " + atomicBoolean.get());
                    fw.flush();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void stop() {
        atomicBoolean.set(false);
    }

    public static void main(String[] args) {
        try {
            HbaseToElastic ea = new HbaseToElastic("aTest", 200, "/home/amirphl/ElasticLog");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
