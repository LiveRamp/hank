package com.rapleaf.hank.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import com.rapleaf.hank.ZkTestCase;

public class TestWatchedLong extends ZkTestCase {
  public void testIt() throws Exception {
    final ZooKeeperPlus zk = getZk();
    final String nodePath = ZkPath.append(getRoot(), "watchedNode");
    zk.create(nodePath, "1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    final WatchedLong wl = new WatchedLong(zk, nodePath);
    assertEquals(Long.valueOf(1), wl.get());

    zk.setData(nodePath, "55".getBytes(), -1);
    Thread.sleep(10);
    assertEquals(Long.valueOf(55), wl.get());

    zk.setData(nodePath, null, -1);
    Thread.sleep(10);
    assertNull(wl.get());

    WatchedLong wl2 = new WatchedLong(zk, nodePath);
    Thread.sleep(10);
    assertNull(wl2.get());
    wl2.set(22L);
    Thread.sleep(10);
    assertEquals(Long.valueOf(22), wl2.get());
    assertEquals(Long.valueOf(22), wl.get());
  }
}
