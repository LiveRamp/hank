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

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.zookeeper.ZkPath;

import java.util.HashMap;
import java.util.Map;

public class TestZkDomainGroupVersion extends ZkTestCase {
  public TestZkDomainGroupVersion() throws Exception {
    super();
  }

  private static final MockDomain domain1 = new MockDomain("domain1", 1, null, null, null, null);
  private static final MockDomain domain2 = new MockDomain("domain2", 1, null, null, null, null);
  private static final MockDomain domain3 = new MockDomain("domain3", 1, null, null, null, null);

  private static final MockDomainGroup mockDomainGroup = new MockDomainGroup("myDomainGroup") {
    @Override
    public Domain getDomain(int domainId) {
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
    ZkDomainGroupVersion dgcv = new ZkDomainGroupVersion(getZk(), versionPath(1), mockDomainGroup);
    assertEquals(1, dgcv.getVersionNumber());
    assertEquals(1, dgcv.getDomainVersions().size());
    assertEquals(domain1, ((DomainGroupVersionDomainVersion) dgcv.getDomainVersions().toArray()[0]).getDomain());

    version(2, 1, 2);
    dgcv = new ZkDomainGroupVersion(getZk(), versionPath(2), mockDomainGroup);
    assertEquals(2, dgcv.getVersionNumber());
    assertEquals(1, dgcv.getDomainVersions().size());
    assertEquals(domain1, ((DomainGroupVersionDomainVersion) dgcv.getDomainVersions().toArray()[0]).getDomain());

    version(3, 1, 3);
    dgcv = new ZkDomainGroupVersion(getZk(), versionPath(3), mockDomainGroup);
    assertEquals(3, dgcv.getVersionNumber());
    assertEquals(1, dgcv.getDomainVersions().size());
    assertEquals(domain1, ((DomainGroupVersionDomainVersion) dgcv.getDomainVersions().toArray()[0]).getDomain());

    version(4, 1, 3, 2, 1);
    dgcv = new ZkDomainGroupVersion(getZk(), versionPath(4), mockDomainGroup);
    assertEquals(4, dgcv.getVersionNumber());
    assertEquals(2, dgcv.getDomainVersions().size());

    try {
      create(versionPath(5));
      dgcv = new ZkDomainGroupVersion(getZk(), versionPath(5), mockDomainGroup);
      fail("should have thrown an error");
    } catch (IllegalStateException e) {
      // success!
    }
  }

  public void testCreateNewSequential() throws Exception {
    Map<String, Integer> map = new HashMap<String, Integer>();
    map.put("domain1", 2);
    map.put("domain4", 7);
    DomainGroup dgc = new MockDomainGroup("blah");
    DomainGroupVersion ver = ZkDomainGroupVersion.create(getZk(), getRoot(), map, dgc);
    assertEquals(0, ver.getVersionNumber());
    assertEquals(2, ver.getDomainVersions().size());
    ver = ZkDomainGroupVersion.create(getZk(), getRoot(), map, dgc);
    assertEquals(1, ver.getVersionNumber());
  }

  private void version(int versionNumber, int... pairs) throws Exception {
    create(versionPath(versionNumber));
    for (int i = 0; i < pairs.length; i += 2) {
      create(ZkPath.create(versionPath(versionNumber), "domain" + pairs[i]), (Integer.toString(pairs[i + 1])));
    }
    create(ZkPath.create(versionPath(versionNumber), ".complete"));
  }

  private String versionPath(int versionNumber) {
    return ZkPath.create(getRoot(), "v" + versionNumber);
  }
}
