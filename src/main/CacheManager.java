package main;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

public class CacheManager {

    private Ignite ignite;

    private IgniteCache<String, String> cache;

    public CacheManager() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(false);
        ignite = Ignition.start(cfg);
        cache = ignite.getOrCreateCache("cache");

        this.ignite = ignite;
        this.cache = cache;
    }

    public void insert(String key, String value) {
        this.cache.put(key, value);
    }

    public String get(String key) {
        return this.cache.get(key);
    }

    public void remove(String key) {
        this.cache.remove(key);
    }

    public int cacheSize() {
        return this.cache.size();
    }
}
