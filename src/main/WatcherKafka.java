package main;

public class WatcherKafka {

    public static void main(String args[]) throws Exception {

        String zkHostPort = "127.0.0.1:2181";
        String znode = "/brokers/topics";

        CacheManager cacheManager = new CacheManager();
        SockerServer server = new SockerServer(6666, cacheManager);
        ZnodeMonitor monitor = new ZnodeMonitor(cacheManager, zkHostPort, znode);
        Resender dispatcher = new Resender(cacheManager);

        // Start server.
        dispatcher.start();
        monitor.start();
        server.start();
    }
}