package main;

import org.apache.zookeeper.AddWatchMode;
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
        this.zk.addWatch(znode, this, AddWatchMode.PERSISTENT_RECURSIVE);
    }

    @Override
    public void process(WatchedEvent event) {
        //System.out.println(event.toString());
        if (event.getType() == Event.EventType.NodeCreated &&
			event.getPath().contains("node")) {
            try {
                count += 1;
                byte[] bytes = zk.getData(event.getPath(), false, null);
		int idSeq = Integer.parseInt(new String(bytes));
		//System.out.println("IdSeq: " + idSeq);
		//System.out.println("Count: " + count);
		//System.out.println("*********************");
                cacheManager.setIdSeq(idSeq);
                cacheManager.addRecived(idSeq);
                cacheManager.setCount(count);
                if (count == cacheManager.getTotal())  {
		    cacheManager.setMonitorFinish(true);
		    cacheManager.setCount(0);
                    count = 0;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
