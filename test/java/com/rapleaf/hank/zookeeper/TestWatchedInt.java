package com.rapleaf.hank.zookeeper;

import com.rapleaf.hank.ZkTestCase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

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
      new WatchedInt(getZk(), ZkPath.append(getRoot(), "watchedNode"), false);
      fail("should have thrown a KeeperException!");
    } catch (KeeperException.NoNodeException e) {
      // expected.
    }

    WatchedInt wi = new WatchedInt(getZk(), ZkPath.append(getRoot(), "watchedNode"), 7);
    assertEquals(Integer.valueOf(7), wi.get());

    wi = new WatchedInt(getZk(), ZkPath.append(getRoot(), "watchedNode"), 10);
    assertEquals(Integer.valueOf(7), wi.get());
  }

  public void testWaitForCreation() throws InterruptedException, KeeperException {
    final ZooKeeperPlus zk = getZk();
    final String nodePath = ZkPath.append(getRoot(), "watchedNode");
    WatchedInt wi;

    // Try not waiting
    try {
      wi = new WatchedInt(zk, nodePath, false);
      fail("Should fail with a NoNodeException");
    } catch (KeeperException.NoNodeException e) {
      // Correct behavior
    }

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          // Wait before creating it
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        try {
          zk.create(nodePath, "42".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });

    // Try waiting
    t.start();
    wi = new WatchedInt(zk, nodePath, true);
    assertEquals(Integer.valueOf(42), wi.get());
  }
}
