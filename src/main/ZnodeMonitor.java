package main;

import java.io.IOException;
import java.util.List;

import static org.apache.zookeeper.AddWatchMode.PERSISTENT_RECURSIVE;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZnodeMonitor extends Thread {

    CacheManager cacheManager;

    String zkUrl;

    String znode;

    ZooKeeper zk;

    List<String> dataList;

    public ZnodeMonitor(CacheManager cacheManager, String zkUrl, String znode) {
        this.cacheManager = cacheManager;
        this.zkUrl = zkUrl;
        this.znode = znode;
    }

    @Override
    public void run(){
        try {
            zk = new ZooKeeper(zkUrl, 60000, null);
            zk.addWatch(znode, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println(event.toString());
                    if (event.getType() == Event.EventType.NodeDataChanged) {
                        try {
                            byte[] bytes = zk.getData(event.getPath(), false, null);
                            String data = new String(bytes);
                            if (event.getPath().contains("node")) {
                                cacheManager.setIdSeq(Integer.parseInt(data.substring(data.lastIndexOf(";") + 1)));
                                cacheManager.addRecived(data);
                            }
                        } catch (InterruptedException | KeeperException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }, PERSISTENT_RECURSIVE);
        } catch (IOException | InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}