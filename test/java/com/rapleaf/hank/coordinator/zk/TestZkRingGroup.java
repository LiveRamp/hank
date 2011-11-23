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
import com.rapleaf.hank.coordinator.mock.MockDomainGroup;
import com.rapleaf.hank.ring_group_conductor.RingGroupConductorMode;
import com.rapleaf.hank.zookeeper.ZkPath;

import java.util.Collections;

public class TestZkRingGroup extends ZkTestCase {

  private Coordinator coordinator;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    create(dg_root);
    create(ring_groups);
    this.coordinator = new MockCoordinator();
  }

  public final class MockRingGroupChangeListener implements RingGroupChangeListener {
    public RingGroup calledWith;

    @Override
    public void onRingGroupChange(RingGroup newRingGroup) {
      this.calledWith = newRingGroup;
      synchronized (this) {
        notifyAll();
      }
    }
  }

  private final String ring_groups = ZkPath.append(getRoot(), "ring_groups");
  private final String ring_group = ZkPath.append(ring_groups, "myRingGroup");
  private final String dg_root = ZkPath.append(getRoot(), "domain_groups");

  public void testLoad() throws Exception {
    create(ring_group, ZkPath.append(dg_root, "myDomainGroup"));
    create(ZkPath.append(ring_group, ZkRingGroup.CURRENT_VERSION_PATH_SEGMENT), Integer.toString(0));
    create(ZkPath.append(ring_group, ZkRingGroup.UPDATING_TO_VERSION_PATH_SEGMENT), Integer.toString(1));
    createRing(1);
    createRing(2);
    createRing(3);

    MockDomainGroup dg = new MockDomainGroup("myDomainGroup");
    ZkRingGroup rg = new ZkRingGroup(getZk(), ring_group, dg, coordinator);

    assertEquals("ring group name", "myRingGroup", rg.getName());
    assertEquals("num rings", 3, rg.getRings().size());
    assertEquals("domain group config", dg, rg.getDomainGroup());

    assertEquals("ring group for localhost:2", 2, rg.getRingForHost(new PartitionServerAddress("localhost", 2)).getRingNumber());
    assertEquals("ring group by number", 3, rg.getRing(3).getRingNumber());

    assertEquals("current version", Integer.valueOf(0), rg.getCurrentVersionNumber());
    assertEquals("current version", Integer.valueOf(1), rg.getUpdatingToVersionNumber());
  }

  public void testVersionStuff() throws Exception {
    ZkDomainGroup dg = (ZkDomainGroup) ZkDomainGroup.create(getZk(), ZkPath.append(getRoot(), "domain_groups"), "blah", null);
    DomainGroupVersion version = dg.createNewVersion(Collections.EMPTY_MAP);
    RingGroup rg = ZkRingGroup.create(getZk(), ZkPath.append(getRoot(), "my_ring_group"), dg, coordinator);
    dumpZk();
    assertNull(rg.getCurrentVersion());
    assertNull(rg.getUpdatingToVersionNumber());
    rg.setUpdatingToVersion(Integer.valueOf(version.getVersionNumber()));
    Thread.sleep(10);
    rg.markUpdateComplete();
    Thread.sleep(10);
    assertEquals(Integer.valueOf(version.getVersionNumber()), rg.getCurrentVersionNumber());
    assertNull(rg.getUpdatingToVersion());
  }

  public void testListener() throws Exception {
    ZkDomainGroup dg = (ZkDomainGroup) ZkDomainGroup.create(getZk(), ZkPath.append(getRoot(), "domain_groups"), "blah", null);
    dg.createNewVersion(Collections.EMPTY_MAP);
    RingGroup rg = ZkRingGroup.create(getZk(), ZkPath.append(getRoot(), "my_ring_group"), dg, coordinator);
    rg.markUpdateComplete();

    MockRingGroupChangeListener listener = new MockRingGroupChangeListener();
    rg.setListener(listener);
    assertNull(listener.calledWith);
    rg.setUpdatingToVersion(2);
    Thread.sleep(10);
    assertNotNull(listener.calledWith);
    assertEquals(Integer.valueOf(2), listener.calledWith.getUpdatingToVersionNumber());

    listener.calledWith = null;
    rg.markUpdateComplete();
    Thread.sleep(10);
    assertNotNull(listener.calledWith);
    assertEquals(Integer.valueOf(2), listener.calledWith.getCurrentVersionNumber());

    listener.calledWith = null;
    Ring newRing = rg.addRing(1);
    Thread.sleep(10);
    assertNotNull(listener.calledWith);
    assertEquals(1, listener.calledWith.getRings().size());
    assertEquals(newRing.getRingNumber(), ((Ring) listener.calledWith.getRings().toArray()[0]).getRingNumber());
  }

  public void testClaimRingGroupConductor() throws Exception {
    ZkDomainGroup dg = (ZkDomainGroup) ZkDomainGroup.create(getZk(), dg_root, "blah", null);
    dg.createNewVersion(Collections.EMPTY_MAP);
    RingGroup rg = ZkRingGroup.create(getZk(), ring_group, dg, coordinator);
    create(ZkPath.append(ring_group, ZkRingGroup.RING_GROUP_CONDUCTOR_ONLINE_PATH_SEGMENT));
    assertFalse(rg.claimRingGroupConductor(RingGroupConductorMode.ACTIVE));
    getZk().delete(ZkPath.append(ring_group, ZkRingGroup.RING_GROUP_CONDUCTOR_ONLINE_PATH_SEGMENT), -1);
    assertTrue(rg.claimRingGroupConductor(RingGroupConductorMode.ACTIVE));
    assertFalse(rg.claimRingGroupConductor(RingGroupConductorMode.ACTIVE));
    rg.releaseRingGroupConductor();
    assertTrue(rg.claimRingGroupConductor(RingGroupConductorMode.ACTIVE));
    Thread.sleep(100);
    assertEquals(RingGroupConductorMode.ACTIVE, rg.getRingGroupConductorMode());
  }

  public void testDelete() throws Exception {
    ZkDomainGroup dg = ZkDomainGroup.create(getZk(), dg_root, "blah", null);
    assertNotNull(getZk().exists(ZkPath.append(dg_root, "blah"), false));
    assertTrue(dg.delete());
    assertNull(getZk().exists(ZkPath.append(dg_root, "blah"), false));
  }

  private void createRing(int ringNum) throws Exception {
    Ring rc = ZkRing.create(getZk(), coordinator, ring_group, ringNum, null);
    rc.addHost(new PartitionServerAddress("localhost", ringNum));
  }
}
