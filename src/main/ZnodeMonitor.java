package main;

import org.apache.zookeeper.*;


public class ZnodeMonitor extends Thread {

    String zkUrl;

    String znode;

    int count = 0;

    CircularList circularList;

    public ZnodeMonitor(String zkUrl, String znode, CircularList circularList) {
        this.zkUrl = zkUrl;
        this.znode = znode;
        this.circularList = circularList;
    }

    public void run() {
        try {
            ZooKeeper zk = new ZooKeeper(zkUrl, 60000, null);
            zk.addWatch(znode, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeCreated &&
                            event.getPath().contains("produce")) {
                        byte[] bytes;
                        try {
                            bytes = zk.getData(event.getPath(), false, null);
                            String data = new String(bytes);
                            count += 1;
                            //System.out.println("Notificação: " + data);
                            //System.out.println("count: " + count);
                            //System.out.println(event.getPath());

                            circularList.addReceived(data);

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }, AddWatchMode.PERSISTENT_RECURSIVE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
