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
package com.liveramp.hank.coordinator.zk;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.coordinator.mock.MockCoordinator;
import com.liveramp.hank.partitioner.ConstantPartitioner;
import com.liveramp.hank.storage.constant.ConstantStorageEngine;
import com.liveramp.hank.test.ZkTestCase;
import com.liveramp.hank.util.Condition;
import com.liveramp.hank.util.WaitUntil;
import com.liveramp.hank.zookeeper.ZkPath;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestZooKeeperCoordinator extends ZkTestCase {
  private final String domains_root = ZkPath.append(getRoot(), "domains");
  private final String domain_groups_root = ZkPath.append(getRoot(), "domain_groups");
  private final String ring_groups_root = ZkPath.append(getRoot(), "ring_groups");
  private ZooKeeperCoordinator coord;

  @Test
  public void testLoad() throws Exception {
    // check standard loading stuff
    assertEquals("number of loaded domain configs", 1, coord.getDomains().size());
    assertEquals("get domain by name", "domain0", coord.getDomain("domain0").getName());

    assertEquals("number of loaded domain group configs", 1, coord.getDomainGroups().size());
    assertEquals("get domain group by name", "myDomainGroup", coord.getDomainGroup("myDomainGroup").getName());

    assertEquals("number of loaded ring groups", 1, coord.getRingGroups().size());
    assertEquals("get ring group by name", "myRingGroup", coord.getRingGroup("myRingGroup").getName());
  }

  @Test
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

  @Test
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

  @Test
  public void testAddRingGroup() throws Exception {
    DomainGroup dg = coord.addDomainGroup("myDomainGroup2");
    Map<Domain, Integer> domainIdToVersion = new HashMap<Domain, Integer>();
    dg.setDomainVersions(domainIdToVersion);
    RingGroup rg = coord.addRingGroup("superDuperRingGroup", "myDomainGroup2");
    assertEquals("superDuperRingGroup", rg.getName());
    assertEquals(0, rg.getRings().size());
  }

  @Test
  public void testDeleteDomain() throws Exception {
    assertNotNull(coord.getDomain("domain0"));
    assertTrue(coord.deleteDomain("domain0"));
    assertNull(coord.getDomain("domain0"));
    assertFalse(coord.deleteDomain("domain0"));
  }

  @Test
  public void testGetDomainShallow() {
    assertNotNull(coord.getDomainShallow("domain0"));
    assertEquals(coord.getDomain("domain0"), coord.getDomainShallow("domain0"));
  }

  @Before
  public void setUp() throws Exception {

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
    return new ZooKeeperCoordinator(getZkConnectString(), 100000000, domains_root, domain_groups_root, ring_groups_root, 5);
  }

  @After
  public void tearDown() throws Exception {
    coord.close();
  }
}
