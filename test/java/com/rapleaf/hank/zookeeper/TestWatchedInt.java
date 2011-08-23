package com.rapleaf.hank.zookeeper;

import com.rapleaf.hank.ZkTestCase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

public class TestWatchedInt extends ZkTestCase {
  public void testIt() throws Exception {
    final ZooKeeperPlus zk = getZk();
    final String nodePath = ZkPath.create(getRoot(), "watchedNode");
    zk.create(nodePath, "1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    final WatchedInt wi = new WatchedInt(zk, nodePath);
    assertEquals(Integer.valueOf(1), wi.get());

    zk.setData(nodePath, "55".getBytes(), -1);
    Thread.sleep(1000);
    assertEquals(Integer.valueOf(55), wi.get());

    zk.setData(nodePath, null, -1);
    Thread.sleep(1000);
    assertNull(wi.get());

    WatchedInt wi2 = new WatchedInt(zk, nodePath);
    assertNull(wi2.get());
    wi2.set(22);
    assertEquals(Integer.valueOf(22), wi2.get());

    Thread.sleep(1000);
    assertEquals(Integer.valueOf(22), wi.get());
  }

  public void testCreate() throws Exception {
    try {
      new WatchedInt(getZk(), ZkPath.create(getRoot(), "watchedNode"));
      fail("should have thrown a KeeperException!");
    } catch (KeeperException.NoNodeException e) {
      // expected.
    }

    WatchedInt wi = new WatchedInt(getZk(), ZkPath.create(getRoot(), "watchedNode"), true, 7);
    assertEquals(Integer.valueOf(7), wi.get());

    wi = new WatchedInt(getZk(), ZkPath.create(getRoot(), "watchedNode"), true, 10);
    assertEquals(Integer.valueOf(7), wi.get());
  }
}
