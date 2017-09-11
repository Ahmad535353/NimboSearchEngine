import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.common.collect.Tuple;
import scala.Tuple2;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public class PageRank {
    public static void main(String[] args){
        //----------------------------- Spark Context -------------------------------------//
        SparkConf con = new SparkConf().setAppName("PageRank").setMaster("spark://server1:7077");
        con.set("spark.hbase.host", "server1").set("spark.deploy.defaultCore","16");
        JavaSparkContext sc = new JavaSparkContext(con);

        // ----------------------------- HBase --------------------------------------------//
        Configuration conf = HBaseConfiguration.create();
        conf.set(TableInputFormat.INPUT_TABLE, "pagerank");
        conf.set("hbase.zookeeper.quorum", "server1");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        long t=System.currentTimeMillis();

        //---------------- Initialize HBase table if necessary ----------------------------//
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
                ImmutableBytesWritable.class, Result.class);
            JavaPairRDD<String,ArrayList<String>> links = hBaseRDD.mapToPair(s -> {
            Tuple2<String,ArrayList<String>> res;
            res=null;
            try {
                res = getPairs(s._2);
            }catch (IOException e){
                e.printStackTrace();
            }
            return res;
        });
        long linksCount = links.count();

        JavaPairRDD<String,Double> ranks = links.mapValues(s->1.0/linksCount);

        for(int i=0;i<50;i++)
        {
            // ----------------------------------- first map -----------------------------------------
            JavaPairRDD<String,Double> joinedRanks=links.join(ranks).values().flatMapToPair(s->{
                ArrayList<Tuple2<String,Double>> res=new ArrayList<>();
                for(int j=0;j<s._1().size();j++)
                {
                    res.add(new Tuple2<>(s._1.get(j), s._2 / s._1.size()));
                }
                return res.iterator();

            });

            // ------------------------------------ first reduce -------------------------------------
            JavaPairRDD<String,Double> sum = joinedRanks.reduceByKey((s1,s2)->(s1+s2));

            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> "+sum.collect());

            // --------------------------------- final map ------------------------------------------

            ranks = sum.mapToPair(s -> new Tuple2<>(s._1 , (s._2 * 0.85)+0.15));
        }


        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> "+ranks.collect());

        sc.stop();
    }

    // -------------------------------- Hbase Results -> J ----------------------------//

    private static Tuple2<String,ArrayList<String>> getPairs(Result result) throws IOException {
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = result.getNoVersionMap();
        NavigableMap<byte[], byte[]> list = map.get(Bytes.toBytes("in"));
        Set<byte []> k =  list.keySet();
        ArrayList<String> keys = new ArrayList<>();
        for( byte [] part : k){
            if(!new String(part).equals("") && !new String(part).equals("redundant-column"))
            keys.add(new String(part));
        }
        return new Tuple2<String,ArrayList<String>>(new String(result.getRow() , "UTF-8") , new ArrayList<String>(keys));
    }
}
