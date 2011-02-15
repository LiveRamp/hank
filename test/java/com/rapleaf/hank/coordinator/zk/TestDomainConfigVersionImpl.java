package com.rapleaf.hank.coordinator.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.config.DomainConfigVersion;

public class TestDomainConfigVersionImpl extends ZkTestCase {
  public TestDomainConfigVersionImpl() throws Exception {
    super();
  }

  public void testLoad() throws Exception {
    getZk().create(getRoot() + "/1", "7".getBytes("utf-8"), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    DomainConfigVersion dcv = new DomainConfigVersionImpl(getZk(), getRoot() + "/1", null);
    assertEquals(7, dcv.getVersionNumber());
  }
}
