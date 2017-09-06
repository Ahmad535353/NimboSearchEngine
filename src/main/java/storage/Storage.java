package storage;

import utils.Pair;

import java.io.IOException;
import java.util.Map;

public interface Storage {

    void addLinks(String url, Map.Entry<String, String>[] links) throws IOException;

    void existsAll(Pair<String, String>[] linkAnchors) throws IOException;
}
