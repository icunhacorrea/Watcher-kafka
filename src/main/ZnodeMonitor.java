package main;

import static org.apache.zookeeper.AddWatchMode.PERSISTENT_RECURSIVE;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZnodeMonitor implements Watcher {

    CacheManager cacheManager;

    ZooKeeper zk;

    int count = 0;

    public ZnodeMonitor(CacheManager cacheManager, String zkUrl, String znode) throws Exception {
        this.cacheManager = cacheManager;
        this.zk = new ZooKeeper(zkUrl, 60000, this);
        this.zk.addWatch(znode, this, PERSISTENT_RECURSIVE);
    }

    @Override
    public void process(WatchedEvent event) {
        //System.out.println(event.toString());
        if (event.getType() == Event.EventType.NodeDataChanged &&
			event.getPath().contains("node")) {
            try {
		count += 1;
                byte[] bytes = zk.getData(event.getPath(), false, null);
                String data = new String(bytes);
		int idSeq = Integer.parseInt(data.substring(data.lastIndexOf(";") + 1));
	        cacheManager.setIdSeq(idSeq);
	        cacheManager.addRecived(data);
		System.out.println("Count -> " + count);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
