package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.config.DomainConfigVersion;

public class TestDomainConfigVersionImpl extends ZkTestCase {
  private final String path = getRoot() + "/myDomain";

  public TestDomainConfigVersionImpl() throws Exception {
    super();
  }

  public void testLoad() throws Exception {
    create(path, "7");
    DomainConfigVersion dcv = new DomainConfigVersionImpl(getZk(), path, null);
    assertEquals(7, dcv.getVersionNumber());
  }
}
