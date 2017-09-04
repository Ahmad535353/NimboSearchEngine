package crawler;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.sun.org.apache.xpath.internal.operations.Bool;
import utils.Constants;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

class LruCache {
    private LoadingCache<String, Boolean> cacheLoader;
    private static LruCache myCache;

    public synchronized static LruCache getInstance() {
        if (myCache == null) {
            myCache = new LruCache();
        }
        return myCache;
    }

    private LruCache() {
        cacheLoader = CacheBuilder.newBuilder().expireAfterWrite(Constants.LRU_TIME_LIMIT, TimeUnit.SECONDS)
                .build(new CacheLoader<String, Boolean>() {
                    @Override
                    public Boolean load(String key) throws Exception {
                        return Boolean.TRUE;
                    }
                });
    }

    public Boolean exist(String domain) {
        Boolean result = cacheLoader.getIfPresent(domain);
        if (result == null) {
            result = false;
        }
        else {
            result = true;
        }
        return result;
    }

    public void get(String domain) {
        try {
            cacheLoader.get(domain);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
