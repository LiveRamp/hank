package com.liveramp.hank.zookeeper;

import java.util.Arrays;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import com.liveramp.hank.test.ZkTestCase;

import static org.junit.Assert.assertTrue;

public class TestZooKeeperPlus extends  ZkTestCase {

  @Test
  public void testIt() throws Exception {

    final ZooKeeperPlus zk = getZk();

    zk.ensureCreated("/", null, CreateMode.PERSISTENT);
    assertExists("/", zk);

    zk.ensureCreated("/simple", "1".getBytes(), CreateMode.PERSISTENT);
    assertExists("/simple", zk);

    zk.ensureCreated("/simple", "2".getBytes(), CreateMode.PERSISTENT);
    assertExists("/simple", zk);
    assertTrue(Arrays.equals(zk.getData("/simple", false, null), "1".getBytes()));

    zk.ensureCreated("/deeper/file", null, CreateMode.PERSISTENT);
    assertExists("/deeper/file", zk);
    assertExists("/deeper", zk);

    zk.ensureCreated("/simple/even/deeper", "3".getBytes(), CreateMode.PERSISTENT);
    assertTrue(Arrays.equals(zk.getData("/simple", false, null), "1".getBytes()));

  }

  private void assertExists(String path, ZooKeeperPlus zkp) throws KeeperException, InterruptedException {
    assertTrue(zkp.exists(path, false) != null);
  }

}
