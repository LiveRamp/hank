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

import java.util.Collections;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.coordinator.DomainGroupConfigVersion;
import com.rapleaf.hank.coordinator.MockDomainGroup;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;
import com.rapleaf.hank.coordinator.RingGroupConfig;

public class TestZkRingGroupConfig extends ZkTestCase {

  public final class MockRingGroupChangeListener implements RingGroupChangeListener {
    public RingGroupConfig calledWith;

    @Override
    public void onRingGroupChange(RingGroupConfig newRingGroup) {
      this.calledWith = newRingGroup;
      synchronized (this) {
        notifyAll();
      }
    }
  }

  private final String ring_groups = getRoot() + "/ring_groups";
  private final String ring_group = ring_groups + "/myRingGroup";
  private final String dg_root = getRoot() + "/domain_groups";

  public void testLoad() throws Exception {
    create(ring_group, dg_root + "/myDomainGroup");
    createRing(1);
    createRing(2);
    createRing(3);

    MockDomainGroup dgc = new MockDomainGroup("myDomainGroup");
    ZkRingGroupConfig ringGroupConf = new ZkRingGroupConfig(getZk(), ring_group, dgc);

    assertEquals("ring group name", "myRingGroup", ringGroupConf.getName());
    assertEquals("num rings", 3, ringGroupConf.getRingConfigs().size());
    assertEquals("domain group config", dgc, ringGroupConf.getDomainGroupConfig());

    assertEquals("ring group for localhost:2", 2, ringGroupConf.getRingConfigForHost(new PartDaemonAddress("localhost", 2)).getRingNumber());
    assertEquals("ring group by number", 3, ringGroupConf.getRingConfig(3).getRingNumber());
  }

  public void testVersionStuff() throws Exception {
    ZkDomainGroup dgc = (ZkDomainGroup) ZkDomainGroup.create(getZk(), getRoot() + "/domain_groups", "blah");
    DomainGroupConfigVersion version = dgc.createNewVersion(Collections.EMPTY_MAP);
    RingGroupConfig rgc = ZkRingGroupConfig.create(getZk(), getRoot() + "/my_ring_group", dgc);
    assertNull(rgc.getCurrentVersion());
    assertEquals(Integer.valueOf(version.getVersionNumber()), rgc.getUpdatingToVersion());
    rgc.updateComplete();
    assertEquals(Integer.valueOf(version.getVersionNumber()), rgc.getCurrentVersion());
    assertNull(rgc.getUpdatingToVersion());
  }

  public void testListener() throws Exception {
    ZkDomainGroup dgc = (ZkDomainGroup) ZkDomainGroup.create(getZk(), getRoot() + "/domain_groups", "blah");
    dgc.createNewVersion(Collections.EMPTY_MAP);
    RingGroupConfig rgc = ZkRingGroupConfig.create(getZk(), getRoot() + "/my_ring_group", dgc);
    rgc.updateComplete();

    MockRingGroupChangeListener listener = new MockRingGroupChangeListener();
    rgc.setListener(listener);
    assertNull(listener.calledWith);
    rgc.setUpdatingToVersion(2);
    synchronized (listener) {
      listener.wait(1000);
    }
    assertNotNull(listener.calledWith);
    assertEquals(Integer.valueOf(2), listener.calledWith.getUpdatingToVersion());

    listener.calledWith = null;
    rgc.updateComplete();
    synchronized (listener) {
      listener.wait(1000);
    }
    assertNotNull(listener.calledWith);
    assertEquals(Integer.valueOf(2), listener.calledWith.getCurrentVersion());

    listener.calledWith = null;
    RingConfig newRing = rgc.addRing(1);
    synchronized (listener) {
      listener.wait(1000);
    }
    assertNotNull(listener.calledWith);
    assertEquals(1, listener.calledWith.getRingConfigs().size());
    assertEquals(newRing.getRingNumber(), ((RingConfig) listener.calledWith.getRingConfigs().toArray()[0]).getRingNumber());
  }

  public void testClaimDataDeployer() throws Exception {
    ZkDomainGroup dgc = (ZkDomainGroup) ZkDomainGroup.create(getZk(), dg_root, "blah");
    dgc.createNewVersion(Collections.EMPTY_MAP);
    RingGroupConfig rgc = ZkRingGroupConfig.create(getZk(), ring_group, dgc);
    create(ring_group + "/data_deployer_online");
    assertFalse(rgc.claimDataDeployer());
    getZk().delete(ring_group + "/data_deployer_online", -1);
    assertTrue(rgc.claimDataDeployer());
    assertFalse(rgc.claimDataDeployer());
    rgc.releaseDataDeployer();
    assertTrue(rgc.claimDataDeployer());
  }

  private void createRing(int ringNum) throws Exception {
    RingConfig rc = ZkRingConfig.create(getZk(), ring_group, ringNum, null, 1);
    rc.addHost(new PartDaemonAddress("localhost", ringNum));
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    create(dg_root);
    create(ring_groups);
  }
}
