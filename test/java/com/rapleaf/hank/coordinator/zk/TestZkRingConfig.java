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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostDomainConfig;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingState;
import com.rapleaf.hank.coordinator.RingStateChangeListener;

public class TestZkRingConfig extends ZkTestCase {
  private static final class MockListener implements RingStateChangeListener {
    public RingConfig calledWith;
    @Override
    public void onRingStateChange(RingConfig ringConfig) {
      calledWith = ringConfig;
      synchronized (this) {
        notifyAll();
      }
    }
  }

  private static final PartDaemonAddress LOCALHOST = PartDaemonAddress.parse("localhost:1");

  private final String ring_group_root = getRoot() + "/ring-group-one";
  private final String ring_root = getRoot() + "/ring-group-one/ring-1";

  public void testCreate() throws Exception {
    ZkRingConfig ringConf = ZkRingConfig.create(getZk(), ring_group_root, 1, null, 1);

    assertEquals("ring number", 1, ringConf.getRingNumber());
    assertNull("version number", ringConf.getVersionNumber());
    assertEquals("updating to version", Integer.valueOf(1), ringConf.getUpdatingToVersionNumber());
    assertEquals("number of hosts", 0, ringConf.getHosts().size());
    assertEquals("initial state", RingState.DOWN, ringConf.getState());
    ringConf.close();
  }

  public void testLoad() throws Exception {
    ZkRingConfig ringConf = ZkRingConfig.create(getZk(), ring_group_root, 1, null, 1);
    ringConf.close();

    ringConf = new ZkRingConfig(getZk(), ring_group_root + "/ring-1", null);

    assertEquals("ring number", 1, ringConf.getRingNumber());
    assertNull("version number", ringConf.getVersionNumber());
    assertEquals("updating to version", Integer.valueOf(1), ringConf.getUpdatingToVersionNumber());
    assertEquals("number of hosts", 0, ringConf.getHosts().size());
    assertEquals("initial state", RingState.DOWN, ringConf.getState());
    ringConf.close();
  }

  public void testUpdatingSemantics() throws Exception {
    ZkRingConfig ringConf = ZkRingConfig.create(getZk(), ring_group_root, 1, null, 1);

    assertTrue("should be updating", ringConf.isUpdatePending());
    assertNull("current version", ringConf.getVersionNumber());
    assertEquals("updating_to_version number", Integer.valueOf(1), ringConf.getUpdatingToVersionNumber());

    ringConf.updateComplete();

    assertFalse("updating", ringConf.isUpdatePending());
    assertEquals("current version", Integer.valueOf(1), ringConf.getVersionNumber());
    assertNull("updating to version", ringConf.getUpdatingToVersionNumber());

    ringConf.setUpdatingToVersion(7);
    assertTrue("should be updating", ringConf.isUpdatePending());
    assertEquals("current version", Integer.valueOf(1), ringConf.getVersionNumber());
    assertEquals("updating_to_version number", Integer.valueOf(7), ringConf.getUpdatingToVersionNumber());

    ringConf.close();
  }

  public void testHosts() throws Exception {
    ZkRingConfig ringConf = ZkRingConfig.create(getZk(), ring_group_root, 1, null, 1);
    assertEquals(0, ringConf.getHosts().size());

    HostConfig hc = ringConf.addHost(LOCALHOST);
    assertEquals(LOCALHOST, hc.getAddress());
    for (int i = 0; i < 20; i++) {
      if (!ringConf.getHosts().isEmpty()){
        break;
      }
      Thread.sleep(100);
    }
    assertEquals(Collections.singleton(hc), ringConf.getHosts());

    assertEquals(LOCALHOST, ringConf.getHostConfigByAddress(LOCALHOST).getAddress());
    ringConf.close();

    // assure that hosts reload well, too
    ringConf = new ZkRingConfig(getZk(), ring_root, null);
    assertEquals(1, ringConf.getHosts().size());

    assertEquals(Collections.singleton(hc), ringConf.getHosts());

    assertEquals(LOCALHOST, ringConf.getHostConfigByAddress(LOCALHOST).getAddress());
    ringConf.close();
  }

  public void testCommandAll() throws Exception {
    ZkRingConfig ringConf = ZkRingConfig.create(getZk(), ring_group_root, 1, null, 1);

    HostConfig hc = ringConf.addHost(LOCALHOST);
    assertNull(hc.getCurrentCommand());

    ringConf.commandAll(HostCommand.SERVE_DATA);
    assertEquals(Arrays.asList(HostCommand.SERVE_DATA), hc.getCommandQueue());
    ringConf.close();
  }

  public void testGetOldestVersionOnHosts() throws Exception {
    ZkRingConfig ringConf = ZkRingConfig.create(getZk(), ring_group_root, 1, null, 1);
    HostConfig hc = ringConf.addHost(LOCALHOST);
    HostDomainConfig d = hc.addDomain(0);
    d.addPartition(1, 1).setCurrentDomainGroupVersion(1);
    d = hc.addDomain(1);
    d.addPartition(1, 2).setCurrentDomainGroupVersion(2);
    assertEquals(Integer.valueOf(1), ringConf.getOldestVersionOnHosts());
    ringConf.close();
  }

  public void testGetHostsForDomainPartition() throws Exception {
    ZkRingConfig rc = ZkRingConfig.create(getZk(), getRoot(), 1, null, 1);
    PartDaemonAddress h1 = new PartDaemonAddress("localhost", 1);
    PartDaemonAddress h2 = new PartDaemonAddress("localhost", 2);
    PartDaemonAddress h3 = new PartDaemonAddress("localhost", 3);

    HostConfig hc1 = rc.addHost(h1);
    HostDomainConfig d = hc1.addDomain(1);
    d.addPartition(0, 1);
    HostConfig hc2 = rc.addHost(h2);
    d = hc2.addDomain(1);
    d.addPartition(1, 1);
    HostConfig hc3 = rc.addHost(h3);
    d = hc3.addDomain(1);
    d.addPartition(2, 1);
    d.addPartition(0, 1);

    assertEquals(new HashSet<HostConfig>(Arrays.asList(hc1, hc3)), rc.getHostsForDomainPartition(1, 0));
    rc.close();
  }

  public void testGetRingState() throws Exception {
    RingConfig rc = ZkRingConfig.create(getZk(), getRoot(), 1, null, 1);
    assertEquals(RingState.DOWN, rc.getState());
    rc.setState(RingState.UP);
    assertEquals(RingState.UP, rc.getState());
    rc = new ZkRingConfig(getZk(), getRoot() + "/ring-1", null);
    assertEquals(RingState.UP, rc.getState());
  }

  public void testGetNumHostsInState() throws Exception {
    RingConfig rc = ZkRingConfig.create(getZk(), getRoot(), 1, null, 1);
    PartDaemonAddress h1 = new PartDaemonAddress("localhost", 1);
    PartDaemonAddress h2 = new PartDaemonAddress("localhost", 2);
    PartDaemonAddress h3 = new PartDaemonAddress("localhost", 3);

    HostConfig hc1 = rc.addHost(h1);
    hc1.setState(HostState.IDLE);
    HostConfig hc2 = rc.addHost(h2);
    hc2.setState(HostState.SERVING);
    HostConfig hc3 = rc.addHost(h3);
    hc3.setState(HostState.OFFLINE);

    // wait for watchers to catch up
    Thread.sleep(2000);

    assertEquals(Collections.singleton(hc1), rc.getHostsInState(HostState.IDLE));
    assertEquals(Collections.singleton(hc2), rc.getHostsInState(HostState.SERVING));
    assertEquals(Collections.singleton(hc3), rc.getHostsInState(HostState.OFFLINE));
  }

  public void testSetUpdatingToVersion() throws Exception {
    RingConfig rc = ZkRingConfig.create(getZk(), getRoot(), 1, null, 1);
    rc.updateComplete();
    assertNull(rc.getUpdatingToVersionNumber());
    rc.setUpdatingToVersion(7);
    assertEquals(Integer.valueOf(7), rc.getUpdatingToVersionNumber());
  }

  public void testRingStateListener() throws Exception {
    RingConfig rc = ZkRingConfig.create(getZk(), getRoot(), 1, null, 1);
    MockListener mockListener = new MockListener();
    rc.setStateChangeListener(mockListener);
    synchronized (mockListener) {
      mockListener.wait(1000);
    }
    assertNull(mockListener.calledWith);
    rc.setState(RingState.DOWN);
    synchronized (mockListener) {
      mockListener.wait(1000);
    }
    assertEquals(rc, mockListener.calledWith);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    create(getRoot() + "/ring-group-one");
//    create(ring_root);
//    create(ring_root + "/hosts");
  }
}
