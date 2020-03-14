package main;

import java.io.IOException;

public class WatcherKafka {

    public static void main(String args[]) throws IOException {

        int port = 6666;

        String zkHostPort = "127.0.0.1:2181";

        String znode = "/brokers/topics/test-topic";

        CacheManager cacheManager = new CacheManager();
        SockerServer server = new SockerServer(6666, cacheManager);
        ZnodeMonitor monitor = new ZnodeMonitor(cacheManager, zkHostPort, znode);

        // Start server.
        //server.start();

    }
}
