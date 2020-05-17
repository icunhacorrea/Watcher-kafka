package main;

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

    private List<Integer> listRecived = new ArrayList<>();

    private int idSeq = 0, total = -1, count = 0;

    private String destino;

    private String origem;

    private AtomicBoolean socketFinish;

    private Long timeoutProduce;

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

    public void addRecived(Integer recieved) {

        synchronized (listRecived){
            setIdSeq(recieved);
            //System.out.println("Message with Key " + recieved + " inserting in list...");
            listRecived.add(recieved);
        }
    }

    public void dispatchList() {

        synchronized (listRecived) {
            if (cacheSize() == 0)
                return;
            listRecived.removeIf(l -> cache.remove(origem + ";" + destino + ";" + l));
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

    public void setOrigem(String origem) {
        this.origem = origem;
    }

    public void setDestino(String destino) {
        this.destino = destino;
    }

    public String getDestino() {
        return destino;
    }

    public String getOrigem() {
        return origem;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setSocketFinish(Boolean finished) {
        this.socketFinish.set(finished);
    }

    public boolean getSocketFinish() {
        return this.socketFinish.get();
    }

    public long getTimeout() {
        return this.timeoutProduce;
    }

    public void startTimeout() {
        this.timeoutProduce = System.currentTimeMillis();
    }

    public void stopTimeout() {
        this.timeoutProduce = 0L;
    }
}

