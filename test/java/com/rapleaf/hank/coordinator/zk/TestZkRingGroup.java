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
import com.rapleaf.hank.zookeeper.ZkPath;

import java.util.Collections;

public class TestZkRingGroup extends ZkTestCase {
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
    createRing(1);
    createRing(2);
    createRing(3);

    MockDomainGroup dg = new MockDomainGroup("myDomainGroup");
    ZkRingGroup rg = new ZkRingGroup(getZk(), ring_group, dg, null);

    assertEquals("ring group name", "myRingGroup", rg.getName());
    assertEquals("num rings", 3, rg.getRings().size());
    assertEquals("domain group config", dg, rg.getDomainGroup());

    assertEquals("ring group for localhost:2", 2, rg.getRingForHost(new PartitionServerAddress("localhost", 2)).getRingNumber());
    assertEquals("ring group by number", 3, rg.getRing(3).getRingNumber());
  }

  public void testVersionStuff() throws Exception {
    ZkDomainGroup dg = (ZkDomainGroup) ZkDomainGroup.create(getZk(), ZkPath.append(getRoot(), "domain_groups"), "blah", null);
    DomainGroupVersion version = dg.createNewVersion(Collections.EMPTY_MAP);
    RingGroup rg = ZkRingGroup.create(getZk(), ZkPath.append(getRoot(), "my_ring_group"), dg, null);
    dumpZk();
    assertNull(rg.getCurrentVersion());
    assertEquals(Integer.valueOf(version.getVersionNumber()), rg.getUpdatingToVersion());
    rg.updateComplete();
    assertEquals(Integer.valueOf(version.getVersionNumber()), rg.getCurrentVersion());
    assertNull(rg.getUpdatingToVersion());
  }

  public void testListener() throws Exception {
    ZkDomainGroup dg = (ZkDomainGroup) ZkDomainGroup.create(getZk(), ZkPath.append(getRoot(), "domain_groups"), "blah", null);
    dg.createNewVersion(Collections.EMPTY_MAP);
    RingGroup rg = ZkRingGroup.create(getZk(), ZkPath.append(getRoot(), "my_ring_group"), dg, null);
    rg.updateComplete();

    MockRingGroupChangeListener listener = new MockRingGroupChangeListener();
    rg.setListener(listener);
    assertNull(listener.calledWith);
    rg.setUpdatingToVersion(2);
    synchronized (listener) {
      listener.wait(1000);
    }
    assertNotNull(listener.calledWith);
    assertEquals(Integer.valueOf(2), listener.calledWith.getUpdatingToVersion());

    listener.calledWith = null;
    rg.updateComplete();
    synchronized (listener) {
      listener.wait(1000);
    }
    assertNotNull(listener.calledWith);
    assertEquals(Integer.valueOf(2), listener.calledWith.getCurrentVersion());

    listener.calledWith = null;
    Ring newRing = rg.addRing(1);
    synchronized (listener) {
      listener.wait(1000);
    }
    assertNotNull(listener.calledWith);
    assertEquals(1, listener.calledWith.getRings().size());
    assertEquals(newRing.getRingNumber(), ((Ring) listener.calledWith.getRings().toArray()[0]).getRingNumber());
  }

  public void testClaimDataDeployer() throws Exception {
    ZkDomainGroup dg = (ZkDomainGroup) ZkDomainGroup.create(getZk(), dg_root, "blah", null);
    dg.createNewVersion(Collections.EMPTY_MAP);
    RingGroup rg = ZkRingGroup.create(getZk(), ring_group, dg, null);
    create(ZkPath.append(ring_group, "data_deployer_online"));
    assertFalse(rg.claimDataDeployer());
    getZk().delete(ZkPath.append(ring_group, "data_deployer_online"), -1);
    assertTrue(rg.claimDataDeployer());
    assertFalse(rg.claimDataDeployer());
    rg.releaseDataDeployer();
    assertTrue(rg.claimDataDeployer());
  }

  public void testDelete() throws Exception {
    ZkDomainGroup dg = (ZkDomainGroup) ZkDomainGroup.create(getZk(), dg_root, "blah", null);
    assertNotNull(getZk().exists(ZkPath.append(dg_root, "blah"), false));
    assertTrue(dg.delete());
    assertNull(getZk().exists(ZkPath.append(dg_root, "blah"), false));
  }

  private void createRing(int ringNum) throws Exception {
    Ring rc = ZkRing.create(getZk(), null, ring_group, ringNum, null, 1);
    rc.addHost(new PartitionServerAddress("localhost", ringNum));
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    create(dg_root);
    create(ring_groups);
  }
}
