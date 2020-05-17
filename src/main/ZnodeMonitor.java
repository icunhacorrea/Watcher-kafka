package main;

import static org.apache.zookeeper.AddWatchMode.PERSISTENT_RECURSIVE;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZnodeMonitor implements Watcher {

    CacheManager cacheManager;

    ZooKeeper zk;


    public ZnodeMonitor(CacheManager cacheManager, String zkUrl, String znode) throws Exception {
        this.cacheManager = cacheManager;
        this.zk = new ZooKeeper(zkUrl, 60000, this);
        this.zk.addWatch(znode, this, PERSISTENT_RECURSIVE);
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event.toString());
        if (event.getType() == Event.EventType.NodeCreated &&
                event.getPath().contains("node")) {
            try {
                byte[] bytes = zk.getData(event.getPath(), false, null);
                String data = new String(bytes);
                cacheManager.setIdSeq(Integer.parseInt(data.substring(data.lastIndexOf(";") + 1)));
                cacheManager.addRecived(data);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}