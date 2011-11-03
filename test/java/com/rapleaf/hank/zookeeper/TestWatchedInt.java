package com.rapleaf.hank.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import com.rapleaf.hank.ZkTestCase;

public class TestWatchedInt extends ZkTestCase {
  public void testIt() throws Exception {
    final ZooKeeperPlus zk = getZk();
    final String nodePath = ZkPath.append(getRoot(), "watchedNode");
    zk.create(nodePath, "1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    final WatchedInt wi = new WatchedInt(zk, nodePath, true);
    assertEquals(Integer.valueOf(1), wi.get());

    zk.setData(nodePath, "55".getBytes(), -1);
    Thread.sleep(1000);
    assertEquals(Integer.valueOf(55), wi.get());

    zk.setData(nodePath, null, -1);
    Thread.sleep(1000);
    assertNull(wi.get());

    WatchedInt wi2 = new WatchedInt(zk, nodePath, true);
    assertNull(wi2.get());
    wi2.set(22);
    assertEquals(Integer.valueOf(22), wi2.get());

    Thread.sleep(1000);
    assertEquals(Integer.valueOf(22), wi.get());
  }

  public void testCreate() throws Exception {
    try {
      new WatchedInt(getZk(), ZkPath.append(getRoot(), "watchedNode"), true);
      fail("should have thrown a KeeperException!");
    } catch (KeeperException.NoNodeException e) {
      // expected.
    }

    WatchedInt wi = new WatchedInt(getZk(), ZkPath.append(getRoot(), "watchedNode"), 7);
    assertEquals(Integer.valueOf(7), wi.get());

    wi = new WatchedInt(getZk(), ZkPath.append(getRoot(), "watchedNode"), 10);
    assertEquals(Integer.valueOf(7), wi.get());
  }
}
