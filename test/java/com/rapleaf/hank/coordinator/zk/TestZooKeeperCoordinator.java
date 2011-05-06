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
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.partitioner.ConstantPartitioner;
import com.rapleaf.hank.storage.constant.ConstantStorageEngine;


public class TestZooKeeperCoordinator extends ZkTestCase {
  private final String domains_root = getRoot() + "/domains";
  private final String domain_groups_root = getRoot() + "/domain_groups";
  private final String ring_groups_root = getRoot() + "/ring_groups";
  private ZooKeeperCoordinator coord;

  public void testLoad() throws Exception {
    // check standard loading stuff
    assertEquals("number of loaded domain configs", 1, coord.getDomainConfigs().size());
    assertEquals("get domain by name", "domain0", coord.getDomainConfig("domain0").getName());

    assertEquals("number of loaded domain group configs", 1, coord.getDomainGroupConfigs().size());
    assertEquals("get domain group by name", "myDomainGroup", coord.getDomainGroupConfig("myDomainGroup").getName());

    assertEquals("number of loaded ring groups", 1, coord.getRingGroups().size());
    assertEquals("get ring group by name", "myRingGroup", coord.getRingGroupConfig("myRingGroup").getName());
  }

  public void testAddDomain() throws Exception {
    coord.addDomain("myDomain", 1234, ConstantStorageEngine.Factory.class.getName(), "---", ConstantPartitioner.class.getName(), 1);
    DomainConfig domainConfig = coord.getDomainConfig("myDomain");
    assertNotNull(domainConfig);
    assertEquals("myDomain", domainConfig.getName());
    assertEquals(1234, domainConfig.getNumParts());
    assertEquals(1, domainConfig.getVersion());
    assertTrue(domainConfig.getStorageEngine() instanceof ConstantStorageEngine);
    assertTrue(domainConfig.getPartitioner() instanceof ConstantPartitioner);
  }

  public void testAddDomainGroup() throws Exception {
    // keep a second coordinator aside
    ZooKeeperCoordinator coord2 = getCoord();
    coord.addDomainGroup("myDomainGroup2");

    DomainGroupConfig c = coord.getDomainGroupConfig("myDomainGroup2");
    assertNotNull(c);
    assertEquals("myDomainGroup2", c.getName());
    assertEquals(0, c.getVersions().size());

    // repeat the assertions with the other coord instance to ensure changes are
    // visible
    for (int i = 0; i < 300; i++) {
      c = coord2.getDomainGroupConfig("myDomainGroup2");
      if (c != null) {
        break;
      }
      Thread.sleep(100);
    }

    assertNotNull("myDomainGroup2 should be found", c);
    assertEquals("myDomainGroup2", c.getName());
    assertEquals(0, c.getVersions().size());

    coord2.close();
  }

  public void testAddRingGroup() throws Exception {
    DomainGroupConfig dg = coord.addDomainGroup("myDomainGroup2");
    Map<String, Integer> domainIdToVersion = new HashMap<String, Integer>();
    dg.createNewVersion(domainIdToVersion);
    RingGroupConfig rg = coord.addRingGroup("superDuperRingGroup", "myDomainGroup2");
    assertEquals("superDuperRingGroup", rg.getName());
    assertEquals(0, rg.getRingConfigs().size());
    assertEquals(Integer.valueOf(dg.getLatestVersion().getVersionNumber()), rg.getUpdatingToVersion());
    assertNull(rg.getCurrentVersion());
  }

  public void testDeleteDomainConfig() throws Exception {
    fail("not implemented");
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    create(domains_root);
    create(domain_groups_root);
    create(ring_groups_root);

    ZkDomainConfig.create(getZk(), domains_root, "domain0", 1, ConstantStorageEngine.Factory.class.getName(), "---", ConstantPartitioner.class.getName(), 1);

    ZkDomainGroupConfig dgc = ZkDomainGroupConfig.create(getZk(), domain_groups_root, "myDomainGroup");
    Map<String, Integer> domainIdToVersion = new HashMap<String, Integer>();
    dgc.createNewVersion(domainIdToVersion);

    ZkRingGroupConfig rg = ZkRingGroupConfig.create(getZk(), ring_groups_root + "/myRingGroup", dgc);
    RingConfig rc = rg.addRing(1);
    rc.addHost(new PartDaemonAddress("localhost", 1));

    coord = getCoord();
  }

  private ZooKeeperCoordinator getCoord() throws Exception {
    return new ZooKeeperCoordinator(getZkConnectString(), 100000000, domains_root, domain_groups_root, ring_groups_root);
  }

  @Override
  protected void tearDown() throws Exception {
    coord.close();

    super.tearDown();
  }
}
