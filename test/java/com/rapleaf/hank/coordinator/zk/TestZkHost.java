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
import com.rapleaf.hank.coordinator.mock.MockDomain;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class TestZkHost extends ZkTestCase {
  private static final PartitionServerAddress ADDRESS = new PartitionServerAddress("my.super.host", 32267);

  public void testCreateAndLoad() throws Exception {
    ZkHost c = ZkHost.create(getZk(), new MockCoordinator(), getRoot(), ADDRESS, null, Collections.<String>emptyList());
    assertEquals(ADDRESS, c.getAddress());
    assertEquals(0, c.getCommandQueue().size());
    assertNull(c.getCurrentCommand());
    assertEquals(HostState.OFFLINE, c.getState());
    assertFalse(Hosts.isOnline(c));

    c.setEphemeralStatistic("a", "A");
    c.setEphemeralStatistic("b", "B");
    Thread.sleep(10);
    assertEquals("A", c.getStatistic("a"));
    assertEquals("B", c.getStatistic("b"));
    assertNull(c.getStatistic("c"));
  }

  public void testStateChangeListener() throws Exception {
    ZkHost c = ZkHost.create(getZk(), new MockCoordinator(), getRoot(), ADDRESS, null, Collections.<String>emptyList());
    MockHostStateChangeListener mockListener = new MockHostStateChangeListener();
    c.setStateChangeListener(mockListener);

    synchronized (mockListener) {
      mockListener.wait(100);
    }

    assertNull("should not receive a callback until something is changed...",
        mockListener.calledWith);

    c.setState(HostState.SERVING);
    synchronized (mockListener) {
      mockListener.wait(1000);
    }
    assertNotNull("mock listener should have received a call!", mockListener.calledWith);
    assertEquals(HostState.SERVING, mockListener.calledWith);
    c.close();
  }

  public void testSetState() throws Exception {
    ZkHost host = ZkHost.create(getZk(), new MockCoordinator(), getRoot(), ADDRESS, null, Collections.<String>emptyList());
    assertEquals(HostState.OFFLINE, host.getState());
    assertFalse(Hosts.isOnline(host));

    host.setState(HostState.IDLE);
    Thread.sleep(100);
    assertEquals(HostState.IDLE, host.getState());
    assertTrue(Hosts.isOnline(host));

    host.setState(HostState.OFFLINE);
    Thread.sleep(100);
    assertEquals(HostState.OFFLINE, host.getState());
    assertFalse(Hosts.isOnline(host));
    host.close();
  }

  public void testCommandQueue() throws Exception {
    ZkHost c = ZkHost.create(getZk(), new MockCoordinator(), getRoot(), ADDRESS, null, Collections.<String>emptyList());
    assertEquals(Collections.EMPTY_LIST, c.getCommandQueue());
    assertNull(c.getCurrentCommand());

    c.enqueueCommand(HostCommand.GO_TO_IDLE);
    assertEquals(Arrays.asList(HostCommand.GO_TO_IDLE), c.getCommandQueue());
    assertNull(c.getCurrentCommand());

    c.enqueueCommand(HostCommand.SERVE_DATA);
    assertEquals(Arrays.asList(HostCommand.GO_TO_IDLE, HostCommand.SERVE_DATA), c.getCommandQueue());
    assertNull(c.getCurrentCommand());

    assertEquals(HostCommand.GO_TO_IDLE, c.nextCommand());
    Thread.sleep(10);
    assertEquals(HostCommand.GO_TO_IDLE, c.getCurrentCommand());
    assertEquals(Arrays.asList(HostCommand.SERVE_DATA), c.getCommandQueue());

    assertEquals(Arrays.asList(HostCommand.SERVE_DATA), c.getCommandQueue());

    c.clearCommandQueue();
    assertEquals(Collections.EMPTY_LIST, c.getCommandQueue());

    c.close();
  }

  public void testCommandQueueListener() throws Exception {
    ZkHost c = ZkHost.create(getZk(), new MockCoordinator(), getRoot(), ADDRESS, null, Collections.<String>emptyList());
    MockHostCommandQueueChangeListener l2 = new MockHostCommandQueueChangeListener();
    c.setCommandQueueChangeListener(l2);
    MockHostStateChangeListener l1 = new MockHostStateChangeListener();
    c.setStateChangeListener(l1);

    assertNull(l1.calledWith);
    assertNull(l2.calledWith);

    c.enqueueCommand(HostCommand.EXECUTE_UPDATE);
    l2.waitForNotification();
    assertNull(l1.calledWith);
    assertEquals(c, l2.calledWith);
    l2.calledWith = null;

    c.enqueueCommand(HostCommand.SERVE_DATA);
    l2.waitForNotification();
    assertNull(l1.calledWith);
    assertEquals(c, l2.calledWith);
    l2.calledWith = null;

    c.enqueueCommand(HostCommand.GO_TO_IDLE);
    l2.waitForNotification();
    assertNull(l1.calledWith);
    assertEquals(c, l2.calledWith);
    l2.calledWith = null;

    c.nextCommand();
    l2.waitForNotification();
    assertNull(l1.calledWith);
    assertEquals(c, l2.calledWith);
    l2.calledWith = null;

    c.nextCommand();
    l2.waitForNotification();
    assertNull(l1.calledWith);
    assertEquals(c, l2.calledWith);
    l2.calledWith = null;

    c.nextCommand();
    l2.waitForNotification();
    assertNull(l1.calledWith);
    assertEquals(c, l2.calledWith);
    l2.calledWith = null;

    c.nextCommand();
    l2.waitForNotification(true);
    assertNull(l1.calledWith);
    assertNull(l2.calledWith);
  }

  private static final Domain d0 = new MockDomain("d0");

  public void testDomains() throws Exception {
    ZkHost c = ZkHost.create(getZk(), new MockCoordinator(), getRoot(), ADDRESS, null, Collections.<String>emptyList());
    assertEquals(0, c.getAssignedDomains().size());

    c.addDomain(d0);
    HostDomain hostDomainConf = (HostDomain) c.getAssignedDomains().toArray()[0];
    assertEquals(0, hostDomainConf.getDomain().getId());
    assertEquals(0, c.getHostDomain(d0).getDomain().getId());
  }

  public void testDuplicateDomainAdd() throws Exception {
    ZkHost c = ZkHost.create(getZk(), new MockCoordinator(), getRoot(), ADDRESS, null, Collections.<String>emptyList());
    c.addDomain(d0);
    try {
      c.addDomain(d0);
      fail("should have thrown an exception!");
    } catch (IOException e) {
      // yay!
    }
  }

  public void testUptime() throws Exception {
    ZkHost c = ZkHost.create(getZk(), new MockCoordinator(), getRoot(), ADDRESS, null, Collections.<String>emptyList());
    assertNull(c.getUpSince());
    final long currentTimeMillis = System.currentTimeMillis();
    c.setState(HostState.IDLE);
    dumpZk();
    Thread.sleep(2000);
    assertNotNull(c.getUpSince());
    assertTrue(c.getUpSince() >= currentTimeMillis);
  }

  private static final Domain d10 = new MockDomain("d10");
  private static final Domain d11 = new MockDomain("d11");
  private static final Domain d12 = new MockDomain("d12");
}
