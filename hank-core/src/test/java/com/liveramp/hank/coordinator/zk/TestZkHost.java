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

import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.HostCommand;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.Hosts;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.mock.MockCoordinator;
import com.liveramp.hank.coordinator.mock.MockDomain;
import com.liveramp.hank.test.ZkTestCase;
import com.liveramp.hank.util.Condition;
import com.liveramp.hank.util.WaitUntil;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestZkHost extends ZkTestCase {

  private static final PartitionServerAddress ADDRESS = new PartitionServerAddress("my.super.host", 32267);
  private static final PartitionServerAddress OTHER_ADDRESS = new PartitionServerAddress("my.other.host", 32267);

  private Coordinator coordinator;
  private Domain d0 = new MockDomain("d0");

  @Before
  public void setUp() throws Exception {
    coordinator = new MockCoordinator() {
      @Override
      public Domain getDomainById(int domainId) {
        if (domainId == 0) {
          return d0;
        } else {
          throw new IllegalStateException();
        }
      }
    };
  }

  @Test
  public void testCreateAndLoad() throws Exception {
    final ZkHost host = ZkHost.create(getZk(), coordinator, getRoot(), ADDRESS, null, Collections.<String>emptyList());
    assertEquals(ADDRESS, host.getAddress());
    assertEquals(0, host.getCommandQueue().size());
    assertNull(host.getCurrentCommand());
    assertEquals(HostState.OFFLINE, host.getState());
    assertFalse(Hosts.isOnline(host));

    host.setEphemeralStatistic("a", "A");
    host.setEphemeralStatistic("b", "B");
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return "A".equals(host.getStatistic("a"))
              && "B".equals(host.getStatistic("b"));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
    assertEquals("A", host.getStatistic("a"));
    assertEquals("B", host.getStatistic("b"));
    assertNull(host.getStatistic("c"));

    host.setAddress(OTHER_ADDRESS);
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return host.getAddress().equals(OTHER_ADDRESS);
      }
    });
    assertEquals(OTHER_ADDRESS, host.getAddress());
  }

  @Test
  public void testStateChangeListener() throws Exception {
    ZkHost host = ZkHost.create(getZk(), coordinator, getRoot(), ADDRESS, null, Collections.<String>emptyList());
    MockHostStateChangeListener mockListener = new MockHostStateChangeListener();
    host.setStateChangeListener(mockListener);

    synchronized (mockListener) {
      mockListener.wait(100);
    }

    assertNull("should not receive a callback until something is changed...",
        mockListener.calledWith);

    host.setState(HostState.SERVING);
    synchronized (mockListener) {
      mockListener.wait(1000);
    }
    assertNotNull("mock listener should have received a call!", mockListener.calledWith);
    assertEquals(HostState.SERVING, mockListener.calledWith);
    host.close();
  }

  @Test
  public void testSetState() throws Exception {
    ZkHost host = ZkHost.create(getZk(), coordinator, getRoot(), ADDRESS, null, Collections.<String>emptyList());
    assertEquals(HostState.OFFLINE, host.getState());
    assertFalse(Hosts.isOnline(host));

    host.setState(HostState.IDLE);
    waitUntilHost(HostState.IDLE, host);
    assertEquals(HostState.IDLE, host.getState());
    assertTrue(Hosts.isOnline(host));

    host.setState(HostState.OFFLINE);
    waitUntilHost(HostState.OFFLINE, host);
    assertEquals(HostState.OFFLINE, host.getState());
    assertFalse(Hosts.isOnline(host));
    host.close();
  }

  @Test
  public void testCommandQueue() throws Exception {
    ZkHost host = ZkHost.create(getZk(), coordinator, getRoot(), ADDRESS, null, Collections.<String>emptyList());
    assertEquals(Collections.EMPTY_LIST, host.getCommandQueue());
    assertNull(host.getCurrentCommand());

    host.enqueueCommand(HostCommand.GO_TO_IDLE);
    assertEquals(Arrays.asList(HostCommand.GO_TO_IDLE), host.getCommandQueue());
    assertNull(host.getCurrentCommand());

    host.enqueueCommand(HostCommand.SERVE_DATA);
    assertEquals(Arrays.asList(HostCommand.GO_TO_IDLE, HostCommand.SERVE_DATA), host.getCommandQueue());
    assertNull(host.getCurrentCommand());

    assertEquals(HostCommand.GO_TO_IDLE, host.nextCommand());
    waitUntilCommand(HostCommand.GO_TO_IDLE, host);
    assertEquals(HostCommand.GO_TO_IDLE, host.getCurrentCommand());
    assertEquals(Arrays.asList(HostCommand.SERVE_DATA), host.getCommandQueue());

    assertEquals(Arrays.asList(HostCommand.SERVE_DATA), host.getCommandQueue());

    host.clearCommandQueue();
    assertEquals(Collections.EMPTY_LIST, host.getCommandQueue());

    host.close();
  }

  @Test
  public void testCommandQueueListener() throws Exception {
    ZkHost host = ZkHost.create(getZk(), coordinator, getRoot(), ADDRESS, null, Collections.<String>emptyList());
    MockHostCommandQueueChangeListener l2 = new MockHostCommandQueueChangeListener();
    host.setCommandQueueChangeListener(l2);
    MockHostStateChangeListener l1 = new MockHostStateChangeListener();
    host.setStateChangeListener(l1);

    assertNull(l1.calledWith);
    assertNull(l2.calledWith);

    host.enqueueCommand(HostCommand.EXECUTE_UPDATE);
    l2.waitForNotification();
    assertNull(l1.calledWith);
    assertEquals(host, l2.calledWith);
    l2.calledWith = null;

    host.enqueueCommand(HostCommand.SERVE_DATA);
    l2.waitForNotification();
    assertNull(l1.calledWith);
    assertEquals(host, l2.calledWith);
    l2.calledWith = null;

    host.enqueueCommand(HostCommand.GO_TO_IDLE);
    l2.waitForNotification();
    assertNull(l1.calledWith);
    assertEquals(host, l2.calledWith);
    l2.calledWith = null;

    host.nextCommand();
    l2.waitForNotification();
    assertNull(l1.calledWith);
    assertEquals(host, l2.calledWith);
    l2.calledWith = null;

    host.nextCommand();
    l2.waitForNotification();
    assertNull(l1.calledWith);
    assertEquals(host, l2.calledWith);
    l2.calledWith = null;

    host.nextCommand();
    l2.waitForNotification();
    assertNull(l1.calledWith);
    assertEquals(host, l2.calledWith);
    l2.calledWith = null;

    host.nextCommand();
    l2.waitForNotification(true);
    assertNull(l1.calledWith);
    assertNull(l2.calledWith);
  }

  @Test
  public void testDomains() throws Exception {
    final ZkHost host = ZkHost.create(getZk(), coordinator, getRoot(), ADDRESS, null, Collections.<String>emptyList());
    assertEquals(0, host.getAssignedDomains().size());

    host.addDomain(d0);
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return !host.getAssignedDomains().isEmpty();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
    HostDomain hostDomain = (HostDomain) host.getAssignedDomains().toArray()[0];
    assertEquals(0, hostDomain.getDomain().getId());
    assertEquals(0, host.getHostDomain(d0).getDomain().getId());
  }

  @Test
  public void testUptime() throws Exception {
    ZkHost host = ZkHost.create(getZk(), coordinator, getRoot(), ADDRESS, null, Collections.<String>emptyList());
    assertNull(host.getUpSince());
    final long currentTimeMillis = System.currentTimeMillis();
    host.setState(HostState.IDLE);
    dumpZk();
    Thread.sleep(2000);
    assertNotNull(host.getUpSince());
    assertTrue(host.getUpSince() >= currentTimeMillis);
  }
}
