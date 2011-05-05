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
import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.HostConfig;
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

    for (int i = 0; i < 300; i++) {
      if (rc.getHosts().size() == 3) {
        break;
      }
      Thread.sleep(100);
    }
    assertEquals(3, rc.getHosts().size());

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

  public void testListenersPreservedWhenHostAdded() throws Exception {
    ZkRingConfig rc = ZkRingConfig.create(getZk(), getRoot() + "/ring-group-one", 1, null, 10);
    HostConfig h1 = rc.addHost(new PartDaemonAddress("localhost", 1));
    MockHostCommandQueueChangeListener l1 = new MockHostCommandQueueChangeListener();
    h1.setCommandQueueChangeListener(l1);
    MockHostStateChangeListener l2 = new MockHostStateChangeListener();
    h1.setStateChangeListener(l2);

    rc.addHost(new PartDaemonAddress("localhost", 2));

    h1.setState(HostState.UPDATING);
    synchronized (l2) {
      l2.wait(WAIT_TIME);
    }
    assertEquals(h1, l2.calledWith);

    h1.enqueueCommand(HostCommand.EXECUTE_UPDATE);
    l1.waitForNotification();
    assertEquals(h1, l1.calledWith);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    create(getRoot() + "/ring-group-one");
  }
}
