package com.liveramp.hank.zookeeper;

import com.liveramp.hank.test.ZkTestCase;
import com.liveramp.hank.util.Condition;
import com.liveramp.hank.util.WaitUntil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;

public class TestWatchedLong extends ZkTestCase {
  @Test
  public void testIt() throws Exception {
    final ZooKeeperPlus zk = getZk();
    final String nodePath = ZkPath.append(getRoot(), "watchedNode");
    zk.create(nodePath, "1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    final WatchedLong wl = new WatchedLong(zk, nodePath);
    assertEquals(Long.valueOf(1), wl.get());

    zk.setData(nodePath, "55".getBytes(), -1);
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return Long.valueOf(55).equals(wl.get());
      }
    });
    assertEquals(Long.valueOf(55), wl.get());

    zk.setData(nodePath, null, -1);
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return wl.get() == null;
      }
    });
    assertNull(wl.get());

    final WatchedLong wl2 = new WatchedLong(zk, nodePath);
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return null == wl2.get();
      }
    });
    assertNull(wl2.get());
    wl2.set(22L);
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return Long.valueOf(22).equals(wl2.get()) && Long.valueOf(22).equals(wl.get());
      }
    });
    assertEquals(Long.valueOf(22), wl2.get());
    assertEquals(Long.valueOf(22), wl.get());
  }
}
