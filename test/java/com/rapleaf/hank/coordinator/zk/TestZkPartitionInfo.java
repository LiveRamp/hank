package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.zookeeper.ZkPath;

public class TestZkPartitionInfo extends ZkTestCase {
  public void testCreate() throws Exception {
    ZkPartitionInfo pi = ZkPartitionInfo.create(getZk(), getRoot(), 1, 15000, 550);
    assertEquals(1, pi.getPartNum());
    assertEquals(15000, pi.getNumBytes());
    assertEquals(550, pi.getNumRecords());

    // should not throw an exception
    pi = ZkPartitionInfo.create(getZk(), getRoot(), 1, 15000, 550);
    assertEquals(1, pi.getPartNum());
    assertEquals(15000, pi.getNumBytes());
    assertEquals(550, pi.getNumRecords());
  }

  public void testLoad() throws Exception {
    ZkPartitionInfo.create(getZk(), getRoot(), 1, 15000, 550);

    ZkPartitionInfo pi = new ZkPartitionInfo(getZk(), ZkPath.append(getRoot(), "part-1"));
    assertEquals(1, pi.getPartNum());
    assertEquals(15000, pi.getNumBytes());
    assertEquals(550, pi.getNumRecords());
  }
}
