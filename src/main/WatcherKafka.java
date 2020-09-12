package main;

public class WatcherKafka {

    public static void main(String args[]) throws Exception {

        String zkHostPort = "localhost:2181";
        //String zkHostPort = "zoo1:2181,zoo2:2181,zoo3:2181";

        String znode = "/brokers/topics";

        CircularList circularList = new CircularList(50);

        ZnodeMonitor monitor = new ZnodeMonitor(zkHostPort, znode, circularList);
        Resender dispatcher = new Resender(circularList);
        SocketServer server = new SocketServer(6666, circularList);

        monitor.start();
        server.start();
        dispatcher.start();
    }
}
