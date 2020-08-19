package main;

public class WatcherKafka {

    public static void main(String args[]) throws Exception {

        //String zkHostPort = "14.0.0.1:2181,14.0.0.3:2181,14.0.0.6:2181";
        String zkHostPort = "zoo1:2181,zoo2:2181,zoo3:2181";

        String znode = "/brokers/topics";

        final boolean IMEDIATE_DISPATCH = true;

        CacheManager cacheManager = new CacheManager("test-topic", "producer-1", IMEDIATE_DISPATCH);
        ZnodeMonitor monitor = new ZnodeMonitor(cacheManager, zkHostPort, znode);
        Resender dispatcher = new Resender(cacheManager);

        monitor.start();
        dispatcher.start();
    }
}
