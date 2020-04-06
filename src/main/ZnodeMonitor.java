package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.zookeeper.AddWatchMode.PERSISTENT_RECURSIVE;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZnodeMonitor implements Watcher {

    CacheManager cacheManager;

    String zkUrl;

    String znode;

    String destiny;

    ZooKeeper zk;

    List<String> dataList;

    public ZnodeMonitor(CacheManager cacheManager, String zkUrl, String znode, String destiny)
            throws IOException, InterruptedException, KeeperException {
        this.cacheManager = cacheManager;
        this.zkUrl = zkUrl;
        this.znode = znode;
        this.dataList = new ArrayList<>();
        this.destiny = destiny;
        zk = new ZooKeeper(zkUrl, 60000, this);
        zk.addWatch(znode, this, PERSISTENT_RECURSIVE);
    }

    @Override
    public void process(WatchedEvent event) {
        //System.out.println(event.toString());
        if (event.getType() == Event.EventType.NodeDataChanged) {
            try {
                byte[] bytes = zk.getData(event.getPath(), false, null);
                String data = new String(bytes);
                if (event.getPath().contains(destiny))
                    cacheManager.addRecived(data);
            } catch (InterruptedException | KeeperException e) {
                e.printStackTrace();
            }
        }
    }
}
