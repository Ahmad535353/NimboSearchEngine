package storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public interface Storage {

    public boolean exists(String rowKey)throws IOException;
    public void addLinks(String url, ArrayList<Map.Entry<String, String>> links) throws IOException;

}
