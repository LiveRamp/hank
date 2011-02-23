package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.ZkTestCase;

public class TestZkHostDomainPartitionConfig extends ZkTestCase {
  public void testIt() throws Exception {
    ZkHostDomainPartitionConfig hdpc = ZkHostDomainPartitionConfig.create(getZk(), getRoot(), 1234, 7);
    assertEquals(1234, hdpc.getPartNum());
    assertNull("current version should be unset", hdpc.getCurrentDomainGroupVersion());
    assertEquals(Integer.valueOf(7), hdpc.getUpdatingToDomainGroupVersion());

    hdpc.setCurrentDomainGroupVersion(7);
    assertEquals(Integer.valueOf(7), hdpc.getCurrentDomainGroupVersion());

    hdpc.setUpdatingToDomainGroupVersion(8);
    assertEquals(Integer.valueOf(8), hdpc.getUpdatingToDomainGroupVersion());

    hdpc.setUpdatingToDomainGroupVersion(null);
    assertNull(hdpc.getUpdatingToDomainGroupVersion());
  }
}
