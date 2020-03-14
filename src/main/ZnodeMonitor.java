package main;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class ZnodeMonitor implements Watcher {

    CacheManager cacheManager;

    String zkUrl;

    String znode;

    ZooKeeper zk;

    public ZnodeMonitor(CacheManager cacheManager, String zkUrl, String znode) {
        this.cacheManager = cacheManager;
        this.zkUrl = zkUrl;
        this.znode = znode;

        try {
            this.zk = new ZooKeeper(this.zkUrl, 60000, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
        String path = event.getPath();
        System.out.println("Oiiii Caminho: " + path);

        if (event.getType() == Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected:
                    System.out.println("SyncConnected.");
                    break;
                case Expired:
                    System.out.println("Expired.");
                    break;
                default:
                    break;
            }
        } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
            System.out.println("Um nó filho foi alterado.");
        }

        initWatch();
    }

    public void initWatch() {
        try {
            System.out.println("Init new watch.");
            Stat stat = zk.exists(znode, this);
            System.out.println(stat.toString());

        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
