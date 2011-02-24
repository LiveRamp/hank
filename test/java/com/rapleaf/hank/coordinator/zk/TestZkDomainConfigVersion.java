package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.coordinator.DomainConfigVersion;

public class TestZkDomainConfigVersion extends ZkTestCase {
  private final String path = getRoot() + "/myDomain";

  public TestZkDomainConfigVersion() throws Exception {
    super();
  }

  public void testLoad() throws Exception {
    create(path, "7");
    DomainConfigVersion dcv = new ZkDomainConfigVersion(getZk(), path, null);
    assertEquals(7, dcv.getVersionNumber());
  }
}
