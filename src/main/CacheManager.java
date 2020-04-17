package main;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.ArrayList;
import java.util.List;

public class CacheManager {

    private Ignite ignite;

    private IgniteCache<String, String> cache;

    private List<String> listRecived = new ArrayList<>();

    private int idSeq = 0;
    private int total = 0;

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

    public void addRecived(String recieved) {

        synchronized (listRecived){
            System.out.println("Message with Key " + recieved + " inserting in list...");
            listRecived.add(recieved);
        }

    }

    public void dispatchList() {

        synchronized (listRecived) {
            System.out.println("Time to dispatch!");
        }

    }

    public void setIdSeq(int idSeq) {
        this.idSeq = idSeq;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getIdSeq() {
        return idSeq;
    }

    public int getTotal() {
        return total;
    }
}
