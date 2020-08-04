package main;

public class WatcherKafka {

    public static void main(String args[]) throws Exception {

        //String zkHostPort = "14.0.0.1:2181,14.0.0.3:2181,14.0.0.6:2181";
        String zkHostPort = "zoo1:2181,zoo2:2182,zoo3:2183";

        String znode = "/brokers";

        CacheManager cacheManager = new CacheManager("test-topic", "producer-1");
        ZnodeMonitor monitor = new ZnodeMonitor(cacheManager, zkHostPort, znode);
        Resender dispatcher = new Resender(cacheManager);

        //SocketServer server = new SocketServer(6666, cacheManager);

        //monitor.setPriority(10);
        //server.setPriority(4);
        //dispatcher.setPriority(4);

        // Start server.
        monitor.start();
        dispatcher.start();
        //server.start();
    }
}
