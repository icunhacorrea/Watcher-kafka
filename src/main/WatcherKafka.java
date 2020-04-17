package main;


import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;


public class WatcherKafka {

    public static void main(String args[]) throws Exception {

        int port = 6666;

        String zkHostPort = "127.0.0.1:2181";
        String znode = "/brokers/topics";

        CacheManager cacheManager = new CacheManager();
        SockerServer server = new SockerServer(6666, cacheManager);
        ZnodeMonitor monitor = new ZnodeMonitor(cacheManager, zkHostPort, znode);

        // Start server.
        server.start();

        while(true) {
            // Ideia de disparar descarte.
            if (cacheManager.cacheSize() > 5) {
                cacheManager.dispatchList();
            }
            Thread.sleep(5000);
        }
    }
}