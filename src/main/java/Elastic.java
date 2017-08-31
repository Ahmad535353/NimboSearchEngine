import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Elastic{
    static Settings settings = Settings.builder()
            .put("cluster.name", "SearchEngine").build();
    static TransportClient client;
    static {
        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("176.31.102.177"), 9300))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("176.31.183.83"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    // ------------------------- Get ---------------------------
    public static GetResponse GetData(String url, String index, String type)
    {
        GetResponse response = client.prepareGet(index, type, url)
                .setOperationThreaded(false)
                .get();
        System.out.println(response.getSourceAsString());
        return response;
    }
    // ----------------------------------------------------------

    // --------------------- index ------------------------------
    public IndexResponse IndexData(String url, String content, String title, String index, String type)
    {
        final byte[] utf8Bytes;
        try {
            utf8Bytes = url.getBytes("UTF-8");
            if(utf8Bytes.length>512)
                return null;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        IndexResponse response=new IndexResponse();
        try {
            XContentBuilder builder;
            builder = jsonBuilder()
                    .startObject()
                    .field("title",title)
                    .field("content", content)
                    .field("prscore",0.0)
                    .field("anchor","")
                    .endObject();
            response = client.prepareIndex(index, type,url)
                    .setSource(builder)
                    .get();
            return response;

        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }
    //-----------------------------------------------------------

    //------------------------ Update ---------------------------
    public static int UpdateData(String url,String value,String title,String index,String type)
    {

        try {
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index(index);
            updateRequest.type(type);
            updateRequest.id(url);
            updateRequest.doc(jsonBuilder()
                    .startObject()
                    .field("title",title)
                    .field("content", value)
                    .field("ahmadScore",0)
                    .field("prscore",0)
                    .endObject());
            client.update(updateRequest).get();
            return 1;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return 1;
    }
    // ---------------------------------------------------------

    // ------------------------- Delete ------------------------
    public static DeleteResponse DeleteData(String url, String index, String type){
        DeleteResponse response = client.prepareDelete(index, type, url)
                .get();
        return response;
    }
    public long DeleteDataByQ(String key,String Val)
    {
        BulkByScrollResponse response =
                DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                        .filter(QueryBuilders.matchQuery(key, Val))
                        .source("persons")
                        .get();
        return response.getDeleted();
    }
    //----------------------------------------------------------

    // ------------------------ Search --------------------------
    public static SearchResponse SearchData(String Str[], String index, String type) {
        //coming soon...
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("message", Str[0]))                 // Query
                //.setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
                .setFrom(0).setSize(60).setExplain(true)
                .get();

        return response;
    }
}
