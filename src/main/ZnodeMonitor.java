package main;

<<<<<<< HEAD
=======
import java.io.IOException;
import java.util.List;

>>>>>>> 3fd7c7dd535f3abfa1fe3c84cde3d3b4d9e87c06
import static org.apache.zookeeper.AddWatchMode.PERSISTENT_RECURSIVE;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZnodeMonitor extends Thread {

    CacheManager cacheManager;

    ZooKeeper zk;

    int count = 0;

<<<<<<< HEAD
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
=======
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
>>>>>>> 3fd7c7dd535f3abfa1fe3c84cde3d3b4d9e87c06
        }
    }
}
