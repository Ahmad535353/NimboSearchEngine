import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Scanner;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
public class SearchUI {
    static Settings settings = Settings.builder()
            .put("cluster.name", "SearchEngine").build();
    static TransportClient client;
    SearchUI(String Elastic1IP,int Elastic1Port,String Elastic2IP,int Elastic2Port) {
        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(Elastic1IP), Elastic1Port))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(Elastic2IP), Elastic2Port));

            UI_Thread ui=new UI_Thread("UI Thread");
            ui.start();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    // ------------------------ Search --------------------------
    static SearchResponse SearchData(String str[],String index,String type) {
        //coming soon...
        BoolQueryBuilder qb = boolQuery();
        for(int i=0;i<str.length;i++)
            qb.should(termQuery("title", str[i]).boost(1.5f));
        for(int i=0;i<str.length;i++)
            qb.should(termQuery("content", str[i]));
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb)                 // Query
//                //.setPostFilter(QueryBuilders.rangeQuery("").from(12).to(18))     // Filter
//                .setFrom(0).setSize(60).setExplain(true)
                .get();

        return response;
    }
    //-----------------------------------------------------------
    public static class UI_Thread implements Runnable {
        private Thread t;
        private String threadName;
        UI_Thread(String name) {
            threadName = name;
            System.out.println("Creating " + threadName);
        }
        public void run() {
            System.out.println("Running " + threadName);
            try {
                Scanner scanner =new Scanner(System.in);
                while(true)
                {

                    String str=scanner.nextLine();
                    str=str.toLowerCase();
                    String words[]=str.split(" ");
                    SearchResponse SR=SearchData(words, "myindex", "mytype");
                    Iterator<SearchHit> a=SR.getHits().iterator();
                    System.out.println(Crawler.UCount);
                    while(a.hasNext())
                    {
                        System.out.println(a.next().getId());
                    }

                }
            } catch (Exception e) {
                System.out.println("Search Thread stopped");
                e.printStackTrace();
            }
            System.out.println(threadName + " exiting.");
        }
        public Thread getT() {
            return t;
        }
        public void start() {
            System.out.println("Starting " + threadName);
            if (t == null) {
                t = new Thread(this, threadName);
                t.start();
            }
        }
    }
}
