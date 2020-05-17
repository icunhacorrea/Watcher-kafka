package main;

public class WatcherKafka {

    public static void main(String args[]) throws Exception {

<<<<<<< HEAD
        int port = 6666;

        String zkHostPort = "14.0.0.1:2181,14.0.0.3:2181,14.0.0.6:2181";
=======
        String zkHostPort = "127.0.0.1:2181";
>>>>>>> 3fd7c7dd535f3abfa1fe3c84cde3d3b4d9e87c06
        String znode = "/brokers/topics";

        CacheManager cacheManager = new CacheManager();
        SocketServer server = new SocketServer(6666, cacheManager);
        ZnodeMonitor monitor = new ZnodeMonitor(cacheManager, zkHostPort, znode);
        Resender dispatcher = new Resender(cacheManager);

        // Start server.
        dispatcher.start();
        monitor.start();
        server.start();
    }
}
