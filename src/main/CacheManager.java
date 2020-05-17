package main;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CacheManager {

    private Ignite ignite;

    private IgniteCache<String, String> cache;

    private List<String> listRecived = new ArrayList<>();

    private int idSeq = 0, total = -1;

    public CacheManager() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(false);
        this.ignite = Ignition.start(cfg);
        this.cache = ignite.getOrCreateCache("cache");
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

    public void removeAll() {
        this.cache.clear();
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
            if (cacheSize() == 0)
                return;
            System.out.println("Start dispatching...");
            listRecived.removeIf(l -> cache.remove(l));
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

    public Collection<?> getAll() {
        return cache.query(new ScanQuery<>()).getAll() ;
    }
}
