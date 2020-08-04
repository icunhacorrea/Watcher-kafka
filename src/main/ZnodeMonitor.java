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
                            event.getPath().contains("produce")) {
                        count += 1;
                        byte[] bytes;
                        try {
                            bytes = zk.getData(event.getPath(), false, null);
                            String data = new String(bytes);
                            int idSeq = Integer.parseInt(data.split(";")[0]);
                            int total = Integer.parseInt(data.split(";")[1]);
                            cacheManager.addRecived(idSeq);

                            //System.out.println("Count: " + count);

                            if ((idSeq + 3 >= total) && cacheManager.getTimeout() == Long.MAX_VALUE) {
                                System.out.println("Proximo de receber a ultima.");
                                cacheManager.startTimeout();
                            }
                            if (idSeq == cacheManager.getTotal())
                                cacheManager.setMonitorFinish(true);
                            if (cacheManager.getTotal() == -1) {
                                cacheManager.setTotal(total);
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
