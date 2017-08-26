import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

class LruCache {
    private static LoadingCache<String,Boolean> cacheLoader;
    LruCache (int timeLimit){

        cacheLoader = CacheBuilder.newBuilder().expireAfterWrite(30, TimeUnit.SECONDS)
                .build(new CacheLoader<String, Boolean>() {
                    @Override
                    public Boolean load(String key) throws Exception {
                        return Boolean.FALSE;
                    }
                });
    }

    public Boolean getIfPresent(String domain) {
        return cacheLoader.getIfPresent(domain);
    }

    public void get(String domain) {
        try {
            cacheLoader.get(domain);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
