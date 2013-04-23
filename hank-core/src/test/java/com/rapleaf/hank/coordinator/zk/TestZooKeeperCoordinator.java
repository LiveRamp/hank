/**
 *  Copyright 2011 LiveRamp
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

import com.rapleaf.hank.test.ZkTestCase;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.coordinator.mock.MockCoordinator;
import com.rapleaf.hank.partitioner.ConstantPartitioner;
import com.rapleaf.hank.storage.constant.ConstantStorageEngine;
import com.rapleaf.hank.util.Condition;
import com.rapleaf.hank.util.WaitUntil;
import com.rapleaf.hank.zookeeper.ZkPath;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class TestZooKeeperCoordinator extends ZkTestCase {
  private final String domains_root = ZkPath.append(getRoot(), "domains");
  private final String domain_groups_root = ZkPath.append(getRoot(), "domain_groups");
  private final String ring_groups_root = ZkPath.append(getRoot(), "ring_groups");
  private ZooKeeperCoordinator coord;

  public void testLoad() throws Exception {
    // check standard loading stuff
    assertEquals("number of loaded domain configs", 1, coord.getDomains().size());
    assertEquals("get domain by name", "domain0", coord.getDomain("domain0").getName());

    assertEquals("number of loaded domain group configs", 1, coord.getDomainGroups().size());
    assertEquals("get domain group by name", "myDomainGroup", coord.getDomainGroup("myDomainGroup").getName());

    assertEquals("number of loaded ring groups", 1, coord.getRingGroups().size());
    assertEquals("get ring group by name", "myRingGroup", coord.getRingGroup("myRingGroup").getName());
  }

  public void testAddDomain() throws Exception {
    coord.addDomain("myDomain", 1234, ConstantStorageEngine.Factory.class.getName(), "---", ConstantPartitioner.class.getName(), Collections.<String>emptyList());
    Domain domain = coord.getDomain("myDomain");
    assertEquals(1, domain.getId());
    assertNotNull(domain);
    assertEquals("myDomain", domain.getName());
    assertEquals(1234, domain.getNumParts());
    assertTrue(domain.getStorageEngine() instanceof ConstantStorageEngine);
    assertTrue(domain.getPartitioner() instanceof ConstantPartitioner);

    coord.addDomain("myDomain2", 1234, ConstantStorageEngine.Factory.class.getName(), "---", ConstantPartitioner.class.getName(), Collections.<String>emptyList());
    domain = coord.getDomain("myDomain2");
    assertEquals(2, domain.getId());
    assertNotNull(domain);
    assertEquals("myDomain2", domain.getName());
    assertEquals(1234, domain.getNumParts());
    assertTrue(domain.getStorageEngine() instanceof ConstantStorageEngine);
    assertTrue(domain.getPartitioner() instanceof ConstantPartitioner);
  }

  public void testAddDomainGroup() throws Exception {
    // keep a second coordinator aside
    final ZooKeeperCoordinator coord2 = getCoord();
    coord.addDomainGroup("myDomainGroup2");

    DomainGroup c = coord.getDomainGroup("myDomainGroup2");
    assertNotNull(c);
    assertEquals("myDomainGroup2", c.getName());
    assertEquals(0, c.getDomainVersions().size());

    // repeat the assertions with the other coord instance to ensure changes are
    // visible

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return coord2.getDomainGroup("myDomainGroup2") != null;
      }
    });

    assertNotNull("myDomainGroup2 should be found", c);
    assertEquals("myDomainGroup2", c.getName());
    assertEquals(0, c.getDomainVersions().size());

    coord2.close();
  }

  public void testAddRingGroup() throws Exception {
    DomainGroup dg = coord.addDomainGroup("myDomainGroup2");
    Map<Domain, Integer> domainIdToVersion = new HashMap<Domain, Integer>();
    dg.setDomainVersions(domainIdToVersion);
    RingGroup rg = coord.addRingGroup("superDuperRingGroup", "myDomainGroup2");
    assertEquals("superDuperRingGroup", rg.getName());
    assertEquals(0, rg.getRings().size());
  }

  public void testDeleteDomain() throws Exception {
    assertNotNull(coord.getDomain("domain0"));
    assertTrue(coord.deleteDomain("domain0"));
    assertNull(coord.getDomain("domain0"));
    assertFalse(coord.deleteDomain("domain0"));
  }

  public void testGetDomainShallow() {
    assertNotNull(coord.getDomainShallow("domain0"));
    assertEquals(coord.getDomain("domain0"), coord.getDomainShallow("domain0"));
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    create(domains_root);
    create(domain_groups_root);
    create(ring_groups_root);

    ZkDomain.create(getZk(), domains_root, "domain0", 1, ConstantStorageEngine.Factory.class.getName(), "---", ConstantPartitioner.class.getName(), 0, Collections.<String>emptyList());

    ZkDomainGroup domainGroup = ZkDomainGroup.create(getZk(), null, domain_groups_root, "myDomainGroup");
    Map<Domain, Integer> domainIdToVersion = new HashMap<Domain, Integer>();
    domainGroup.setDomainVersions(domainIdToVersion);

    ZkRingGroup rg = ZkRingGroup.create(getZk(), ring_groups_root + "/myRingGroup", domainGroup, new MockCoordinator());
    Ring rc = rg.addRing(1);
    rc.addHost(new PartitionServerAddress("localhost", 1), Collections.<String>emptyList());

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
