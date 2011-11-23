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
import com.rapleaf.hank.zookeeper.ZkPath;
import org.apache.zookeeper.KeeperException;

import java.util.Collections;

public class TestZkRing extends ZkTestCase {

  private static Coordinator coordinator;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    create(ZkPath.append(getRoot(), "ring-group-one"));
    TestZkRing.coordinator = new MockCoordinator();
  }

  private static final class MockListener implements RingStateChangeListener {
    public Ring calledWith;

    @Override
    public void onRingStateChange(Ring ringConfig) {
      calledWith = ringConfig;
      synchronized (this) {
        notifyAll();
      }
    }
  }

  private static final PartitionServerAddress LOCALHOST = PartitionServerAddress.parse("localhost:1");

  private final String ring_group_root = ZkPath.append(getRoot(), "ring-group-one");
  private final String ring_root = ZkPath.append(getRoot(), "ring-group-one/ring-1");

  public void testCreate() throws Exception {
    ZkRing ring = ZkRing.create(getZk(), coordinator, ring_group_root, 1, null);

    assertEquals("ring number", 1, ring.getRingNumber());
    assertNull("version number", ring.getCurrentVersionNumber());
    assertNull("updating to version", ring.getUpdatingToVersionNumber());
    assertEquals("number of hosts", 0, ring.getHosts().size());
    assertEquals("initial state", RingState.CLOSED, ring.getState());
    ring.close();
  }

  public void testLoad() throws Exception {
    ZkRing ring = ZkRing.create(getZk(), coordinator, ring_group_root, 1, null);
    ring.close();

    ring = new ZkRing(getZk(), ZkPath.append(ring_group_root, "ring-1"), null, coordinator);

    assertEquals("ring number", 1, ring.getRingNumber());
    assertNull("version number", ring.getCurrentVersionNumber());
    assertNull("updating to version", ring.getUpdatingToVersionNumber());
    assertEquals("number of hosts", 0, ring.getHosts().size());
    assertEquals("initial state", RingState.CLOSED, ring.getState());
    ring.close();
  }

  public void testUpdatingSemantics() throws Exception {
    ZkRing ring = ZkRing.create(getZk(), coordinator, ring_group_root, 1, null);

    assertNull("updating_to_version number", ring.getUpdatingToVersionNumber());

    ring.setUpdatingToVersion(1);
    Thread.sleep(10);

    assertEquals("updating_to_version number", Integer.valueOf(1), ring.getUpdatingToVersionNumber());
    assertTrue("should be updating", Rings.isUpdatePending(ring));
    assertNull("current version", ring.getCurrentVersionNumber());

    ring.markUpdateComplete();
    Thread.sleep(10);

    assertFalse("updating", Rings.isUpdatePending(ring));
    assertEquals("current version", Integer.valueOf(1), ring.getCurrentVersionNumber());
    assertNull("updating to version", ring.getUpdatingToVersionNumber());

    ring.setUpdatingToVersion(7);
    Thread.sleep(10);

    assertTrue("should be updating", Rings.isUpdatePending(ring));
    assertEquals("current version", Integer.valueOf(1), ring.getCurrentVersionNumber());
    assertEquals("updating_to_version number", Integer.valueOf(7),
        ring.getUpdatingToVersionNumber());

    ring.close();
  }

  public void testHosts() throws Exception {
    ZkRing ring = ZkRing.create(getZk(), coordinator, ring_group_root, 1, null);
    assertEquals(0, ring.getHosts().size());

    Host host = ring.addHost(LOCALHOST);
    assertEquals(LOCALHOST, host.getAddress());
    for (int i = 0; i < 20; i++) {
      if (!ring.getHosts().isEmpty()) {
        break;
      }
      Thread.sleep(100);
    }
    assertEquals(Collections.singleton(host), ring.getHosts());

    assertEquals(LOCALHOST, ring.getHostByAddress(LOCALHOST).getAddress());
    ring.close();

    // assure that hosts reload well, too
    ring = new ZkRing(getZk(), ring_root, null, coordinator);
    assertEquals(1, ring.getHosts().size());

    assertEquals(Collections.singleton(host), ring.getHosts());

    assertEquals(LOCALHOST, ring.getHostByAddress(LOCALHOST).getAddress());

    assertTrue(ring.removeHost(LOCALHOST));
    assertNull(ring.getHostByAddress(LOCALHOST));
    assertFalse(ring.removeHost(LOCALHOST));

    ring.close();
  }

  public void testGetRingState() throws Exception {
    Ring ring = ZkRing.create(getZk(), coordinator, getRoot(), 1, null);
    assertEquals(RingState.CLOSED, ring.getState());
    ring.setState(RingState.OPEN);
    assertEquals(RingState.OPEN, ring.getState());
    ring = new ZkRing(getZk(), ZkPath.append(getRoot(), "ring-1"), null, coordinator);
    assertEquals(RingState.OPEN, ring.getState());
  }

  public void testSetUpdatingToVersion() throws Exception {
    Ring ring = ZkRing.create(getZk(), coordinator, getRoot(), 1, null);
    Thread.sleep(10);
    ring.markUpdateComplete();
    Thread.sleep(10);
    assertNull(ring.getUpdatingToVersionNumber());
    ring.setUpdatingToVersion(7);
    Thread.sleep(10);
    assertEquals(Integer.valueOf(7), ring.getUpdatingToVersionNumber());
  }

  public void testRingStateListener() throws Exception {
    Ring ring = ZkRing.create(getZk(), coordinator, getRoot(), 1, null);
    MockListener mockListener = new MockListener();
    ring.setStateChangeListener(mockListener);
    synchronized (mockListener) {
      mockListener.wait(1000);
    }
    assertNull(mockListener.calledWith);
    ring.setState(RingState.CLOSED);
    synchronized (mockListener) {
      mockListener.wait(1000);
    }
    assertEquals(ring, mockListener.calledWith);
  }

  public void testListenersPreservedWhenHostAdded() throws Exception {
    ZkRing ring = ZkRing.create(getZk(), coordinator, ZkPath.append(getRoot(), "ring-group-one"), 1, null);
    Host h1 = ring.addHost(new PartitionServerAddress("localhost", 1));
    MockHostCommandQueueChangeListener l1 = new MockHostCommandQueueChangeListener();
    h1.setCommandQueueChangeListener(l1);
    MockHostStateChangeListener l2 = new MockHostStateChangeListener();
    h1.setStateChangeListener(l2);

    ring.addHost(new PartitionServerAddress("localhost", 2));

    h1.setState(HostState.UPDATING);
    synchronized (l2) {
      l2.wait(WAIT_TIME);
    }
    assertEquals(HostState.UPDATING, l2.calledWith);

    h1.enqueueCommand(HostCommand.EXECUTE_UPDATE);
    l1.waitForNotification();
    assertEquals(h1, l1.calledWith);
  }

  public void testDelete() throws Exception {
    ZkRing ring = ZkRing.create(getZk(), coordinator, ZkPath.append(getRoot(), "ring-group-one"), 1, null);
    ring.delete();
    try {
      new ZkRing(getZk(), ZkPath.append(getRoot(), "ring-group-one/ring-1"), null, coordinator);
      fail("should have had an exception!");
    } catch (KeeperException.NoNodeException e) {
      // expected
    }
  }
}
