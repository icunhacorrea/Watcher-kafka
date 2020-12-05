package main;

public class WatcherKafka {

    public static void main(String args[]) throws Exception {

        //String zkHostPort = "localhost:2181";
        //String zkHostPort = "zoo1:2181,zoo2:2181,zoo3:2181,zoo4:2181";
        String zkHostPort = "172.21.0.2:2181,172.21.0.3:2181,172.21.0.4:2181,172.21.0.10:2181";

        String znode = "/brokers/topics";

        CircularList circularList = new CircularList(15000);

        ZnodeMonitor monitor = new ZnodeMonitor(zkHostPort, znode, circularList);
        SocketServer server = new SocketServer(6666, circularList);
        Controller dispatcher = new Controller(circularList);

        monitor.setPriority(10);
        server.setPriority(10);
        dispatcher.setPriority(10);

        monitor.start();
        server.start();
        dispatcher.start();
    }
}
