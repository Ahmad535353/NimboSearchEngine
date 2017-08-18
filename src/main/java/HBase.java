import com.google.common.net.InternetDomainName;

import java.util.HashMap;

public class HBase {
    private HashMap<String , String> urlsStorage = new HashMap<String, String>();
    public void addParsedData(String url , String text , String title ){
        urlsStorage.put(url, url);
    }
    public boolean check(String url){
        return urlsStorage.containsKey(url);
    }
}
