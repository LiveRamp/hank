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
import com.rapleaf.hank.coordinator.mock.MockCoordinator;
import com.rapleaf.hank.partitioner.ConstantPartitioner;
import com.rapleaf.hank.partitioner.Murmur64Partitioner;
import com.rapleaf.hank.storage.constant.ConstantStorageEngine;
import com.rapleaf.hank.storage.curly.Curly;
import com.rapleaf.hank.zookeeper.ZkPath;
import org.apache.zookeeper.KeeperException;

import java.util.HashMap;
import java.util.Map;

public class TestZkDomainGroup extends ZkTestCase {
  public class MockDomainGroupChangeListener implements DomainGroupChangeListener {
    public DomainGroup calledWith;

    @Override
    public void onDomainGroupChange(DomainGroup newDomainGroup) {
      this.calledWith = newDomainGroup;
      synchronized (this) {
        notifyAll();
      }
    }
  }

  private final String dg_root = ZkPath.append(getRoot(), "myDomainGroup");
  private final String domains_root = ZkPath.append(getRoot(), "domains");

  public void testLoad() throws Exception {
    final Domain d0 = ZkDomain.create(getZk(), domains_root, "domain0", 1024, Curly.Factory.class.getName(), "---", Murmur64Partitioner.class.getName(), 0);
    final Domain d1 = ZkDomain.create(getZk(), domains_root, "domain1", 1024, Curly.Factory.class.getName(), "---", Murmur64Partitioner.class.getName(), 1);

    Coordinator coord = new MockCoordinator() {
      @Override
      public Domain getDomain(String domainName) {
        if (domainName.equals("domain0")) {
          return d0;
        } else {
          return d1;
        }
      }
    };

    create(ZkPath.append(dg_root, "versions"));
    create(ZkPath.append(dg_root, "versions/v1"));
    create(ZkPath.append(dg_root, "versions/v1/domain0"), "1");
    create(ZkPath.append(dg_root, "versions/v1/domain1"), "1");
    create(ZkPath.append(dg_root, "versions/v1/" + DotComplete.NODE_NAME), "1");
    create(ZkPath.append(dg_root, "versions/v2"));
    create(ZkPath.append(dg_root, "versions/v2/domain0"), "1");
    create(ZkPath.append(dg_root, "versions/v2/domain1"), "1");

    ZkDomainGroup dg = new ZkDomainGroup(getZk(), dg_root, coord);

    assertEquals(1, dg.getVersions().size());
    assertEquals(1, ((DomainGroupVersion) dg.getVersions().toArray()[0]).getVersionNumber());
    assertEquals(1, DomainGroupUtils.getLatestVersion(dg).getVersionNumber());
  }

  public void testDomainsAndListener() throws Exception {
    final Domain d0 = createDomain("domain0");
    final Domain d1 = createDomain("domain1");

    Coordinator coord = new MockCoordinator() {
      @Override
      public Domain getDomain(String domainName) {
        if (domainName.equals("domain0")) {
          return d0;
        } else {
          return d1;
        }
      }
    };

    DomainGroup dgc = ZkDomainGroup.create(getZk(), dg_root, "myDomainGroup", coord);
    MockDomainGroupChangeListener listener = new MockDomainGroupChangeListener();
    dgc.setListener(listener);
    assertNull(listener.calledWith);

    assertNull(listener.calledWith);

    Map<Domain, Integer> versionMap = new HashMap<Domain, Integer>() {{
      put(d0, 1);
      put(d1, 3);
    }};
    dgc.createNewVersion(versionMap);

    synchronized (listener) {
      listener.wait(1000);
    }
    assertNotNull(listener.calledWith);
    assertEquals(dgc.getName(), listener.calledWith.getName());
  }

  public void testDelete() throws Exception {
    ZkDomainGroup dg = ZkDomainGroup.create(getZk(), dg_root, "myDomainGroup", null);
    assertNotNull(getZk().exists(dg.getPath(), false));
    assertTrue(dg.delete());
    assertNull(getZk().exists(dg.getPath(), false));
  }

  private Domain createDomain(String domainName) throws KeeperException, InterruptedException {
    return ZkDomain.create(getZk(),
        domains_root,
        domainName,
        1,
        ConstantStorageEngine.Factory.class.getName(),
        "---\n",
        ConstantPartitioner.class.getName(),
        0);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    create(domains_root);
    create(dg_root);
  }
}
