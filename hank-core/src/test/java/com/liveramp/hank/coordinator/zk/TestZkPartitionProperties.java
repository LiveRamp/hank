package com.liveramp.hank.coordinator.zk;

import com.liveramp.hank.test.ZkTestCase;
import com.liveramp.hank.zookeeper.ZkPath;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestZkPartitionProperties extends ZkTestCase {
  @Test
  public void testCreate() throws Exception {
    ZkPartitionProperties pi = ZkPartitionProperties.create(getZk(), getRoot(), 1, 15000, 550);
    assertEquals(1, pi.getPartitionNumber());
    assertEquals(15000, pi.getNumBytes());
    assertEquals(550, pi.getNumRecords());

    // should not throw an exception
    pi = ZkPartitionProperties.create(getZk(), getRoot(), 1, 15000, 550);
    assertEquals(1, pi.getPartitionNumber());
    assertEquals(15000, pi.getNumBytes());
    assertEquals(550, pi.getNumRecords());
  }

  @Test
  public void testLoad() throws Exception {
    ZkPartitionProperties.create(getZk(), getRoot(), 1, 15000, 550);

    ZkPartitionProperties pi = new ZkPartitionProperties(getZk(), ZkPath.append(getRoot(), "part-1"));
    assertEquals(1, pi.getPartitionNumber());
    assertEquals(15000, pi.getNumBytes());
    assertEquals(550, pi.getNumRecords());
  }
}
