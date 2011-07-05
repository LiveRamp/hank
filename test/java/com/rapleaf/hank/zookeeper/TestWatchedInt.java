package com.rapleaf.hank.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import com.rapleaf.hank.ZkTestCase;

public class TestWatchedInt extends ZkTestCase {
  public void testIt() throws Exception {
    final ZooKeeperPlus zk = getZk();
    final String nodePath = getRoot() + "/watchedNode";
    zk.create(nodePath, "1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    final WatchedInt wi = new WatchedInt(zk, nodePath);
    assertEquals(Integer.valueOf(1), wi.get());

    zk.setData(nodePath, "55".getBytes(), -1);
    Thread.sleep(1000);
    assertEquals(Integer.valueOf(55), wi.get());

    zk.setData(nodePath, null, -1);
    Thread.sleep(1000);
    assertNull(wi.get());
  }
}
