package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.coordinator.HostDomainConfig;
import com.rapleaf.hank.coordinator.HostDomainPartitionConfig;

public class TestZkHostDomainConfig extends ZkTestCase {

  public void testIt() throws Exception {
    HostDomainConfig hdc = ZkHostDomainConfig.create(getZk(), getRoot(), (byte) 1);
    assertEquals(1, hdc.getDomainId());
    assertEquals(0, hdc.getPartitions().size());

    hdc.addPartition(1, 1);
    assertEquals(1, hdc.getPartitions().size());
    assertEquals(1, ((HostDomainPartitionConfig)hdc.getPartitions().toArray()[0]).getPartNum());
  }
}
