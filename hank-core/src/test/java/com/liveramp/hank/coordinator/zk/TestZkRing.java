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
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostCommand;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.mock.MockCoordinator;
import com.liveramp.hank.test.ZkTestCase;
import com.liveramp.hank.util.Condition;
import com.liveramp.hank.util.WaitUntil;
import com.liveramp.hank.zookeeper.ZkPath;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestZkRing extends ZkTestCase {

  private static Coordinator coordinator;

  @Before
  public void setUp() throws Exception {
    create(ZkPath.append(getRoot(), "ring-group-one"));
    TestZkRing.coordinator = new MockCoordinator();
  }

  private static final PartitionServerAddress LOCALHOST = PartitionServerAddress.parse("localhost:1");
  private static final PartitionServerAddress LOCALHOST2 = PartitionServerAddress.parse("localhost:2");

  private final String ring_group_root = ZkPath.append(getRoot(), "ring-group-one");
  private final String ring_root = ZkPath.append(getRoot(), "ring-group-one/ring-1");

  @Test
  public void testCreate() throws Exception {
    ZkRing ring = ZkRing.create(getZk(), coordinator, ring_group_root, 1, null, null);

    assertEquals("ring number", 1, ring.getRingNumber());
    assertEquals("number of hosts", 0, ring.getHosts().size());
    ring.close();
  }

  @Test
  public void testLoad() throws Exception {
    ZkRing ring = ZkRing.create(getZk(), coordinator, ring_group_root, 1, null, null);
    ring.close();

    ring = new ZkRing(getZk(), ZkPath.append(ring_group_root, "ring-1"), null, coordinator, null);

    assertEquals("ring number", 1, ring.getRingNumber());
    assertEquals("number of hosts", 0, ring.getHosts().size());
    ring.close();
  }

  @Test
  public void testHosts() throws Exception {
    final ZkRing ring = ZkRing.create(getZk(), coordinator, ring_group_root, 1, null, null);
    assertEquals(0, ring.getHosts().size());

    Host host = ring.addHost(LOCALHOST, Collections.<String>emptyList());
    assertEquals(LOCALHOST, host.getAddress());

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return !ring.getHosts().isEmpty();
      }
    });
    assertEquals(Collections.singleton(host), ring.getHosts());

    assertEquals(LOCALHOST, ring.getHostByAddress(LOCALHOST).getAddress());
    ring.close();

    // assure that hosts reload well, too
    final ZkRing sameRing = new ZkRing(getZk(), ring_root, null, coordinator, null);
    assertEquals(1, sameRing.getHosts().size());

    assertEquals(Collections.singleton(host), sameRing.getHosts());

    assertEquals(LOCALHOST, sameRing.getHostByAddress(LOCALHOST).getAddress());

    // Rename that host
    sameRing.getHostByAddress(LOCALHOST).setAddress(LOCALHOST2);

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return sameRing.getHostByAddress(LOCALHOST2) != null &&
            LOCALHOST2.equals(sameRing.getHostByAddress(LOCALHOST2).getAddress());
      }
    });

    assertEquals(LOCALHOST2, sameRing.getHostByAddress(LOCALHOST2).getAddress());

    assertNull(sameRing.getHostByAddress(LOCALHOST));

    assertTrue(sameRing.removeHost(LOCALHOST2));
    assertNull(sameRing.getHostByAddress(LOCALHOST2));
    assertFalse(sameRing.removeHost(LOCALHOST2));

    ring.close();
  }

  @Test
  public void testListenersPreservedWhenHostAdded() throws Exception {
    ZkRing ring = ZkRing.create(getZk(), coordinator, ZkPath.append(getRoot(), "ring-group-one"), 1, null, null);
    Host h1 = ring.addHost(new PartitionServerAddress("localhost", 1), Collections.<String>emptyList());
    MockHostCommandQueueChangeListener l1 = new MockHostCommandQueueChangeListener();
    h1.setCommandQueueChangeListener(l1);
    final MockHostStateChangeListener l2 = new MockHostStateChangeListener();
    h1.setStateChangeListener(l2);

    ring.addHost(new PartitionServerAddress("localhost", 2), Collections.<String>emptyList());

    h1.setState(HostState.UPDATING);
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return HostState.UPDATING == l2.calledWith;
      }
    });

    assertEquals(HostState.UPDATING, l2.calledWith);

    h1.enqueueCommand(HostCommand.EXECUTE_UPDATE);
    l1.waitForNotification();
    assertEquals(h1, l1.calledWith);
  }

  @Test
  public void testDelete() throws Exception {
    ZkRing ring = ZkRing.create(getZk(), coordinator, ZkPath.append(getRoot(), "ring-group-one"), 1, null, null);
    ring.delete();
    assertTrue(getZk().exists(ZkPath.append(getRoot(), "ring-group-one/ring-1"), null) == null);
  }
}
