package com.rapleaf.hank.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.zookeeper.WatchedMap.ElementLoader;

public class TestWatchedMap extends ZkTestCase {
  public void testIt() throws Exception {
//    Logger.getLogger("org.apache.zookeeper").setLevel(Level.ALL);

    final ZooKeeperPlus zk = getZk();
    final String colRoot = getRoot() + "/collection";
    zk.create(colRoot, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    final ElementLoader<String> elementLoader = new ElementLoader<String>() {
      @Override
      public String load(ZooKeeperPlus zk, String basePath, String relPath) {
        try {
          return new String(zk.getData(basePath + "/" + relPath, false, new Stat()));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
    final WatchedMap<String> c1 = new WatchedMap<String>(zk, colRoot, elementLoader);
    dumpZk();
    assertEquals(0, c1.size());
//    zk.create(colRoot + "/first", "data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//    assertEquals(1, c1.size());
  }
}
