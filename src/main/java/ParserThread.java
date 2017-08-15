import com.google.common.cache.LoadingCache;
import org.jsoup.select.Elements;

import java.io.PrintWriter;
import java.util.concurrent.ArrayBlockingQueue;

public class ParserThread implements Runnable{
    private Thread thread;
    private LoadingCache<String,Boolean> cacheLoader;
    private ArrayBlockingQueue<String > q;
    public int time;
    public void run() {
        org.jsoup.nodes.Document doc = null;
        Elements elements = null;
    }
    public ParserThread(LoadingCache cache1, ArrayBlockingQueue q1){
        this.cacheLoader = cache1;
        this.q = q1;
        thread = new Thread(this);
        thread.start();
    }
}
