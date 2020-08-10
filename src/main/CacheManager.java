package main;

import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class CacheManager {

    private Ignite ignite;

    private IgniteCache<String, String> cache;

    private DataStorageMetrics pm;

    private List<Integer> listRecived = new ArrayList<>();

    private int idSeq = 0, total = -1;

    private String destino;

    private String origem;

    private AtomicBoolean monitorFinish = new AtomicBoolean(false);

    private long timeoutProduce = Long.MAX_VALUE;

    public CacheManager(String destino, String origem) {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(false);
        this.ignite = Ignition.start(cfg);
        this.pm = this.ignite.dataStorageMetrics();
        this.cache = ignite.createCache("cache");
        this.destino = destino;
        this.origem = origem;
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

    public void addRecived(Integer recieved) {

        synchronized (listRecived){
            //System.out.println("Message with Key " + recieved + " inserting in list...");
            //System.out.println(origem + ";" + destino + ";" + recieved);
            listRecived.add(recieved);
        }
    }

    public void dispatchList() {

        synchronized (listRecived) {
            System.out.println("Dispatching...");
            if (cacheSize() == 0)
                return;
            listRecived.removeIf(l -> cache.remove(origem + ";" + destino + ";" + l));
        }
    }

    public int getRecievedSize() {
        return listRecived.size();
    }

    public void clearRecieved() {
        listRecived.clear();
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

    public void setMonitorFinish(Boolean finished) {
        this.monitorFinish.set(finished);
    }

    public boolean getMonitorFinish() {
        return this.monitorFinish.get();
    }

    public long getTimeout() {
        return this.timeoutProduce;
    }

    public void startTimeout() {
        this.timeoutProduce = System.currentTimeMillis();
    }

    public void stopTimeout() {
        this.timeoutProduce = Long.MAX_VALUE;
    }

    public String cacheToString() {
        return this.cache.toString();
    }

    public void showMemoryUsage() {
        System.out.println("TotalAllocatedSize: " + pm.getTotalAllocatedSize());
        System.out.println("Off-Heap Size: " + pm.getOffHeapSize());
        System.out.println("Off-Heap Used: " + pm.getOffheapUsedSize());
    }
}

