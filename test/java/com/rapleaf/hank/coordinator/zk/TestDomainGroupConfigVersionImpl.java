package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.config.DomainConfig;
import com.rapleaf.hank.config.DomainConfigVersion;
import com.rapleaf.hank.config.MockDomainConfig;
import com.rapleaf.hank.config.MockDomainGroupConfig;
import com.rapleaf.hank.exception.DataNotFoundException;

public class TestDomainGroupConfigVersionImpl extends ZkTestCase {
  public TestDomainGroupConfigVersionImpl() throws Exception {
    super();
  }

  private static final MockDomainConfig domain1 = new MockDomainConfig("domain1", 1, null, null, 5);
  private static final MockDomainConfig domain2 = new MockDomainConfig("domain2", 1, null, null, 5);
  private static final MockDomainConfig domain3 = new MockDomainConfig("domain3", 1, null, null, 5);

  private static final MockDomainGroupConfig mockDomainGroup = new MockDomainGroupConfig("myDomainGroup") {
    @Override
    public DomainConfig getDomainConfig(int domainId) throws DataNotFoundException {
      switch (domainId) {
        case 1:
          return domain1;
        case 2:
          return domain2;
        case 3:
          return domain3;
        default:
          return null;
      }
    }

    @Override
    public int getDomainId(String domainName) throws DataNotFoundException {
      if (domainName.equals("domain1")) {
        return 1;
      } else if (domainName.equals("domain2")) {
        return 2;
      }
      throw new IllegalStateException();
    }
  };

  public void testLoad() throws Exception {
    version(1, 1, 1);
    DomainGroupConfigVersionImpl dgcv = new DomainGroupConfigVersionImpl(getZk(), versionPath(1), mockDomainGroup);
    assertEquals(1, dgcv.getVersionNumber());
    assertEquals(1, dgcv.getDomainConfigVersions().size());
    assertEquals(domain1, ((DomainConfigVersion)dgcv.getDomainConfigVersions().toArray()[0]).getDomainConfig());

    version(2, 1, 2);
    dgcv = new DomainGroupConfigVersionImpl(getZk(), versionPath(2), mockDomainGroup);
    assertEquals(2, dgcv.getVersionNumber());
    assertEquals(1, dgcv.getDomainConfigVersions().size());
    assertEquals(domain1, ((DomainConfigVersion)dgcv.getDomainConfigVersions().toArray()[0]).getDomainConfig());

    version(3, 1, 3);
    dgcv = new DomainGroupConfigVersionImpl(getZk(), versionPath(3), mockDomainGroup);
    assertEquals(3, dgcv.getVersionNumber());
    assertEquals(1, dgcv.getDomainConfigVersions().size());
    assertEquals(domain1, ((DomainConfigVersion)dgcv.getDomainConfigVersions().toArray()[0]).getDomainConfig());

    version(4, 1, 3, 2, 1);
    dgcv = new DomainGroupConfigVersionImpl(getZk(), versionPath(4), mockDomainGroup);
    assertEquals(4, dgcv.getVersionNumber());
    assertEquals(2, dgcv.getDomainConfigVersions().size());

    try {
      create(versionPath(5));
      dgcv = new DomainGroupConfigVersionImpl(getZk(), versionPath(5), mockDomainGroup);
      fail("should have thrown an error");
    } catch (IllegalStateException e) {
      // success!
    }
  }

  private void version(int versionNumber, int... pairs) throws Exception {
    create(versionPath(versionNumber));
    for (int i = 0; i < pairs.length; i+=2) {
      create(versionPath(versionNumber) + "/domain" + pairs[i], ("" + pairs[i+1]));
    }
    create(versionPath(versionNumber) + "/.complete");
  }

  private String versionPath(int versionNumber) {
    return getRoot() + "/" + versionNumber;
  }
}
