/**
 *  Copyright 2011 Rapleaf
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.rapleaf.hank.coordinator.zk;

import java.util.HashMap;
import java.util.Map;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainGroupVersionDomainVersion;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfigVersion;
import com.rapleaf.hank.coordinator.MockDomainGroupConfig;
import com.rapleaf.hank.coordinator.mock.MockDomainConfig;

public class TestZkDomainGroupConfigVersion extends ZkTestCase {
  public TestZkDomainGroupConfigVersion() throws Exception {
    super();
  }

  private static final MockDomainConfig domain1 = new MockDomainConfig("domain1", 1, null, null, 5);
  private static final MockDomainConfig domain2 = new MockDomainConfig("domain2", 1, null, null, 5);
  private static final MockDomainConfig domain3 = new MockDomainConfig("domain3", 1, null, null, 5);

  private static final MockDomainGroupConfig mockDomainGroup = new MockDomainGroupConfig("myDomainGroup") {
    @Override
    public DomainConfig getDomainConfig(int domainId) {
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
    public Integer getDomainId(String domainName) {
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
    ZkDomainGroupConfigVersion dgcv = new ZkDomainGroupConfigVersion(getZk(), versionPath(1), mockDomainGroup);
    assertEquals(1, dgcv.getVersionNumber());
    assertEquals(1, dgcv.getDomainConfigVersions().size());
    assertEquals(domain1, ((DomainGroupVersionDomainVersion)dgcv.getDomainConfigVersions().toArray()[0]).getDomainConfig());

    version(2, 1, 2);
    dgcv = new ZkDomainGroupConfigVersion(getZk(), versionPath(2), mockDomainGroup);
    assertEquals(2, dgcv.getVersionNumber());
    assertEquals(1, dgcv.getDomainConfigVersions().size());
    assertEquals(domain1, ((DomainGroupVersionDomainVersion)dgcv.getDomainConfigVersions().toArray()[0]).getDomainConfig());

    version(3, 1, 3);
    dgcv = new ZkDomainGroupConfigVersion(getZk(), versionPath(3), mockDomainGroup);
    assertEquals(3, dgcv.getVersionNumber());
    assertEquals(1, dgcv.getDomainConfigVersions().size());
    assertEquals(domain1, ((DomainGroupVersionDomainVersion)dgcv.getDomainConfigVersions().toArray()[0]).getDomainConfig());

    version(4, 1, 3, 2, 1);
    dgcv = new ZkDomainGroupConfigVersion(getZk(), versionPath(4), mockDomainGroup);
    assertEquals(4, dgcv.getVersionNumber());
    assertEquals(2, dgcv.getDomainConfigVersions().size());

    try {
      create(versionPath(5));
      dgcv = new ZkDomainGroupConfigVersion(getZk(), versionPath(5), mockDomainGroup);
      fail("should have thrown an error");
    } catch (IllegalStateException e) {
      // success!
    }
  }

  public void testCreateNewSequential() throws Exception {
    Map<String, Integer> map = new HashMap<String, Integer>();
    map.put("domain1", 2);
    map.put("domain4", 7);
    DomainGroupConfig dgc = new MockDomainGroupConfig("blah");
    DomainGroupConfigVersion ver = ZkDomainGroupConfigVersion.create(getZk(), getRoot(), map, dgc);
    assertEquals(0, ver.getVersionNumber());
    assertEquals(2, ver.getDomainConfigVersions().size());
    ver = ZkDomainGroupConfigVersion.create(getZk(), getRoot(), map, dgc);
    assertEquals(1, ver.getVersionNumber());
  }

  private void version(int versionNumber, int... pairs) throws Exception {
    create(versionPath(versionNumber));
    for (int i = 0; i < pairs.length; i+=2) {
      create(versionPath(versionNumber) + "/domain" + pairs[i], ("" + pairs[i+1]));
    }
    create(versionPath(versionNumber) + "/.complete");
  }

  private String versionPath(int versionNumber) {
    return getRoot() + "/v" + versionNumber;
  }
}
