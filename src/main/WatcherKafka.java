package main;

public class WatcherKafka {

    public static void main(String args[]) throws Exception {

        String zkHostPort = "localhost:2181";
        String znode = "/brokers/topics";

        CacheManager cacheManager = new CacheManager();
        SocketServer server = new SocketServer(6666, cacheManager);
        ZnodeMonitor monitor = new ZnodeMonitor(cacheManager, zkHostPort, znode);
        Resender dispatcher = new Resender(cacheManager);

        // Start server.
        dispatcher.start();
        server.start();
    }
}