package main;

import org.apache.zookeeper.*;


public class ZnodeMonitor extends Thread {

    CacheManager cacheManager;

    String zkUrl;

    String znode;

    int count = 0;

    public ZnodeMonitor(CacheManager cacheManager, String zkUrl, String znode) {
        this.cacheManager = cacheManager;
        this.zkUrl = zkUrl;
        this.znode = znode;
    }

    public void run() {
        try {
            ZooKeeper zk = new ZooKeeper(zkUrl, 60000, null);
            zk.addWatch(znode, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeCreated &&
                            event.getPath().contains("node")) {
                        count += 1;
                        byte[] bytes;
                        try {
                            bytes = zk.getData(event.getPath(), false, null);
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
            }, AddWatchMode.PERSISTENT_RECURSIVE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
