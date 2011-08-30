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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.HostDomain;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.PartitionServerAddress;

public class TestZkHost extends ZkTestCase {
  private static final PartitionServerAddress ADDRESS = new PartitionServerAddress("my.super.host", 32267);

  public void testCreateAndLoad() throws Exception {
    ZkHost c = ZkHost.create(getZk(), getRoot(), ADDRESS);
    assertEquals(ADDRESS, c.getAddress());
    assertEquals(0, c.getCommandQueue().size());
    assertNull(c.getCurrentCommand());
    assertEquals(HostState.OFFLINE, c.getState());
    assertFalse(c.isOnline());
  }

  public void testStateChangeListener() throws Exception {
    ZkHost c = ZkHost.create(getZk(), getRoot(), ADDRESS);
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
    assertEquals(ADDRESS, mockListener.calledWith.getAddress());
    assertEquals(HostState.SERVING, mockListener.calledWith.getState());
    c.close();
  }

  public void testSetState() throws Exception {
    ZkHost host = ZkHost.create(getZk(), getRoot(), ADDRESS);
    assertEquals(HostState.OFFLINE, host.getState());
    assertFalse(host.isOnline());

    host.setState(HostState.IDLE);
    assertEquals(HostState.IDLE, host.getState());
    assertTrue(host.isOnline());

    host.setState(HostState.OFFLINE);
    assertEquals(HostState.OFFLINE, host.getState());
    assertFalse(host.isOnline());
    host.close();
  }

  public void testCommandQueue() throws Exception {
    ZkHost c = ZkHost.create(getZk(), getRoot(), ADDRESS);
    assertEquals(Collections.EMPTY_LIST, c.getCommandQueue());
    assertNull(c.getCurrentCommand());

    c.enqueueCommand(HostCommand.GO_TO_IDLE);
    assertEquals(Arrays.asList(HostCommand.GO_TO_IDLE), c.getCommandQueue());
    assertNull(c.getCurrentCommand());

    c.enqueueCommand(HostCommand.SERVE_DATA);
    assertEquals(Arrays.asList(HostCommand.GO_TO_IDLE, HostCommand.SERVE_DATA), c.getCommandQueue());
    assertNull(c.getCurrentCommand());

    assertEquals(HostCommand.GO_TO_IDLE, c.processNextCommand());
    assertEquals(HostCommand.GO_TO_IDLE, c.getCurrentCommand());
    assertEquals(Arrays.asList(HostCommand.SERVE_DATA), c.getCommandQueue());

    c.completeCommand();
    assertNull(c.getCurrentCommand());
    assertEquals(Arrays.asList(HostCommand.SERVE_DATA), c.getCommandQueue());

    c.clearCommandQueue();
    assertEquals(Collections.EMPTY_LIST, c.getCommandQueue());

    c.close();
  }

  public void testCommandQueueListener() throws Exception {
    ZkHost c = ZkHost.create(getZk(), getRoot(), ADDRESS);
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

    c.processNextCommand();
    l2.waitForNotification();
    assertNull(l1.calledWith);
    assertEquals(c, l2.calledWith);
    l2.calledWith = null;

    c.processNextCommand();
    l2.waitForNotification();
    assertNull(l1.calledWith);
    assertEquals(c, l2.calledWith);
    l2.calledWith = null;

    c.processNextCommand();
    l2.waitForNotification();
    assertNull(l1.calledWith);
    assertEquals(c, l2.calledWith);
    l2.calledWith = null;

    c.processNextCommand();
    l2.waitForNotification(true);
    assertNull(l1.calledWith);
    assertNull(l2.calledWith);
  }

  public void testDomains() throws Exception {
    ZkHost c = ZkHost.create(getZk(), getRoot(), ADDRESS);
    assertEquals(0, c.getAssignedDomains().size());

    c.addDomain(0);
    HostDomain hostDomainConf = (HostDomain) c.getAssignedDomains().toArray()[0];
    assertEquals(0, hostDomainConf.getDomain());

    assertEquals(0, c.getHostDomain(0).getDomain());
  }

  public void testDuplicateDomainAdd() throws Exception {
    ZkHost c = ZkHost.create(getZk(), getRoot(), ADDRESS);
    c.addDomain(0);
    try {
      c.addDomain(0);
      fail("should have thrown an exception!");
    } catch (IOException e) {
      // yay!
    }
  }

  public void testCounters() throws Exception {
    ZkHost c = ZkHost.create(getZk(), getRoot(), ADDRESS);
    c.addDomain(10).addPartition(9, 8).setCount("Unicorns", 17);
    c.addDomain(11).addPartition(3, 2).setCount("Unicorns", 13);
    c.addDomain(12).addPartition(1, 1).setCount("Centaurs", 5);
    Set<String> aggregateCountKeys = new HashSet<String>() {
      {
        add("Unicorns");
        add("Centaurs");
      }
    };
    assertEquals(5, c.getAggregateCount("Centaurs").intValue());
    assertEquals(30, c.getAggregateCount("Unicorns").intValue());
    assertNull(c.getAggregateCount("Gargoyles"));
    assertNotNull(c.getAggregateCountKeys().equals(aggregateCountKeys));
  }
}
