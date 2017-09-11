import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class PageRank {
    public static void main(String[] args) throws IOException {
        //----------------------------- Spark Context -------------------------------------//
        SparkConf con = new SparkConf().setAppName("PageRank").setMaster("spark://server1:7077");
        con.set("spark.hbase.host", "server1").set("spark.deploy.defaultCore", "16");
        JavaSparkContext sc = new JavaSparkContext(con);

        // ----------------------------- HBase --------------------------------------------//
        Configuration conf = HBaseConfiguration.create();
        conf.set(TableInputFormat.INPUT_TABLE, "la");
        conf.set("hbase.zookeeper.quorum", "server1");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        long t = System.currentTimeMillis();

        //---------------- Initialize HBase table if necessary ----------------------------//
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
                ImmutableBytesWritable.class, Result.class);
        JavaPairRDD<String, ArrayList<String>> links = hBaseRDD.mapToPair(s -> {
            Tuple2<String, ArrayList<String>> res;
            res = null;
            try {
                res = getPairs(s._2);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return res;
        });
        long linksCount = links.count();
        Double incrementValue = 0.15 / linksCount;

        JavaPairRDD<String, Double> ranks = links.mapValues(s -> (1.0 / linksCount));

        for (int i = 0; i < 50; i++) {
            // ----------------------------------- first map -----------------------------------------
            JavaPairRDD<String, Double> joinedRanks = links.join(ranks).values().flatMapToPair(s -> {
                ArrayList<Tuple2<String, Double>> res = new ArrayList<>();
                for (int j = 0; j < s._1().size(); j++) {
                    res.add(new Tuple2<>(s._1.get(j), s._2 / s._1.size()));
                }
                return res.iterator();

            });

            // ------------------------------------ first reduce -------------------------------------
            JavaPairRDD<String, Double> sum = joinedRanks.reduceByKey((s1, s2) -> (s1 + s2));

//            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> "+sum.collect());

            // --------------------------------- final map ------------------------------------------

            ranks = sum.mapToPair(s -> new Tuple2<>(s._1, (0.85 * s._2) + incrementValue));
        }


        Configuration config = null;
        try {
            config = new Configuration();
            config.set("hbase.zookeeper.quorum", "server1");
            config.set("hbase.zookeeper.property.clientPort", "2181");
            config.set(TableInputFormat.INPUT_TABLE, "res");
            //config.set("hbase.master", "127.0.0.1:60000");
            HBaseAdmin.checkHBaseAvailable(config);
            System.out.println("HBase is running!");
        } catch (MasterNotRunningException e) {
            System.out.println("HBase is not running!");
            sc.stop();
            System.exit(1);
        } catch (Exception ce) {
            ce.printStackTrace();
        }

        Job newAPIJobConfiguration1 = Job.getInstance(config);
        newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "res");
        newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = ranks.mapToPair(new PairFunction<Tuple2<String, Double>, ImmutableBytesWritable, Put>() {
            @Override
            public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, Double> t) throws Exception {
                Put put = new Put(Bytes.toBytes(t._1()));
                put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("r"), Bytes.toBytes(t._2().doubleValue()));
                return new Tuple2<>(new ImmutableBytesWritable(), put);
            }
        });

        // save to HBase- Spark built-in API method
        hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());

        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> " + ranks.collect());

        sc.stop();
    }

    // -------------------------------- Hbase Results -> J ----------------------------//

    private static Tuple2<String, ArrayList<String>> getPairs(Result result) throws IOException {
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = result.getNoVersionMap();
        NavigableMap<byte[], byte[]> list = map.get(Bytes.toBytes("la"));
        Set<byte[]> k = list.keySet();
        ArrayList<String> keys = new ArrayList<>();
        for (byte[] part : k) {
            if (!new String(part).equals("") && !new String(part).equals("redundant-column"))
                keys.add(new String(part));
        }
        return new Tuple2<String, ArrayList<String>>(new String(result.getRow(), "UTF-8"), new ArrayList<String>(keys));
    }
}