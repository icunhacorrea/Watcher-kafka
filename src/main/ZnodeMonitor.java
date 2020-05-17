package main;

import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZnodeMonitor implements Watcher {

    CacheManager cacheManager;

    ZooKeeper zk;

    private int count = 0;

    public ZnodeMonitor(CacheManager cacheManager, String zkUrl, String znode) throws Exception {
        this.cacheManager = cacheManager;
        this.zk = new ZooKeeper(zkUrl, 60000, this);
        this.zk.addWatch(znode, this, AddWatchMode.PERSISTENT_RECURSIVE);
    }

    @Override
    public void process(WatchedEvent event) {
        //System.out.println(event.toString());
        if (event.getType() == Event.EventType.NodeCreated &&
			event.getPath().contains("node")) {
            try {
		    String pathToCast = event.getPath().substring(event.getPath().lastIndexOf("/") + 1);
		    if (!pathToCast.contains("node")) {
			    count += 1;
			    int idSeq = Integer.parseInt(pathToCast);
			    cacheManager.setIdSeq(idSeq);
			    cacheManager.addRecived(idSeq);
			    if (idSeq == cacheManager.getTotal())
				count = 0;
			    System.out.println("Count -> " + count);
		    }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    void setCount(int count) {
        this.count = count;
    }
}
