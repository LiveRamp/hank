/**
 * Copyright 2011 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.liveramp.hank.ring_group_conductor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainAndVersion;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostCommand;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostDomainPartition;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.mock.MockDomain;
import com.liveramp.hank.coordinator.mock.MockDomainGroup;
import com.liveramp.hank.partition_assigner.ModPartitionAssigner;
import com.liveramp.hank.partition_assigner.PartitionAssigner;
import com.liveramp.hank.test.BaseTestCase;
import com.liveramp.hank.test.coordinator.MockHost;
import com.liveramp.hank.test.coordinator.MockRing;
import com.liveramp.hank.test.coordinator.MockRingGroup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestRingGroupUpdateTransitionFunctionImpl extends BaseTestCase {

  private class MockRingLocal extends MockRing {

    public MockRingLocal(int number,
                         Set<Host> hosts) {
      super(hosts, null, number);
    }
  }

  private class MockRingGroupLocal extends MockRingGroup {

    public MockRingGroupLocal(Ring... rings) {
      super(domainGroup, "myRingGroup", new LinkedHashSet<Ring>(Arrays.asList(rings)));
    }
  }

  private class MockHostLocal extends MockHost {

    public MockHostLocal(PartitionServerAddress address) {
      super(address);
    }

    public void setCurrentVersion(Set<DomainAndVersion> currentVersions) throws IOException {
      for (HostDomain hostDomain : getAssignedDomains()) {
        Integer version = null;
        for (DomainAndVersion domainVersion : currentVersions) {
          if (domainVersion.getDomain().equals(hostDomain.getDomain())) {
            version = domainVersion.getVersionNumber();
            break;
          }
        }
        if (version != null) {
          for (HostDomainPartition partition : hostDomain.getPartitions()) {
            partition.setCurrentDomainVersion(version);
          }
        }
      }
    }
  }

  private static Map<Domain, Integer> versionsMap1 = new HashMap<Domain, Integer>();
  private static Map<Domain, Integer> versionsMap2 = new HashMap<Domain, Integer>();
  private static Map<Domain, Integer> versionsMap3 = new HashMap<Domain, Integer>();
  private static Set<DomainAndVersion> v1 = new HashSet<DomainAndVersion>();
  private static Set<DomainAndVersion> v2 = new HashSet<DomainAndVersion>();
  private static Set<DomainAndVersion> v3 = new HashSet<DomainAndVersion>();

  private static Domain domain1 = new MockDomain("domain1", 1, 6, null, null, null, null);
  private static Domain domain2 = new MockDomain("domain2", 2, 6, null, null, null, null);

  private static DomainGroup domainGroup = new MockDomainGroup("myDomainGroup");

  private MockRingLocal r0;
  private MockRingLocal r1;
  private MockRingLocal r2;

  private MockHostLocal r0h0 = null;
  private MockHostLocal r0h1 = null;
  private MockHostLocal r1h0 = null;
  private MockHostLocal r1h1 = null;
  private MockHostLocal r2h0 = null;
  private MockHostLocal r2h1 = null;

  private MockRingGroupLocal rg = null;
  private PartitionAssigner partitionAssigner;
  private RingGroupUpdateTransitionFunctionImpl testTransitionFunction = null;

  @Before
  public void setUp() throws Exception {

    r0h0 = new MockHostLocal(new PartitionServerAddress("localhost", 1));
    r0h1 = new MockHostLocal(new PartitionServerAddress("localhost", 2));
    r1h0 = new MockHostLocal(new PartitionServerAddress("localhost", 3));
    r1h1 = new MockHostLocal(new PartitionServerAddress("localhost", 4));
    r2h0 = new MockHostLocal(new PartitionServerAddress("localhost", 5));
    r2h1 = new MockHostLocal(new PartitionServerAddress("localhost", 6));

    Set<Host> r0Hosts = new HashSet<Host>();
    r0Hosts.add(r0h0);
    r0Hosts.add(r0h1);
    Set<Host> r1Hosts = new HashSet<Host>();
    r1Hosts.add(r1h0);
    r1Hosts.add(r1h1);
    Set<Host> r2Hosts = new HashSet<Host>();
    r2Hosts.add(r2h0);
    r2Hosts.add(r2h1);

    r0 = new MockRingLocal(0, r0Hosts);
    r1 = new MockRingLocal(1, r1Hosts);
    r2 = new MockRingLocal(2, r2Hosts);

    rg = new MockRingGroupLocal(r0, r1, r2);

    partitionAssigner = new ModPartitionAssigner();

    testTransitionFunction = new RingGroupUpdateTransitionFunctionImpl(partitionAssigner, 0, 2, 0, 0, 0, null);

    // V1
    versionsMap1.put(domain1, 1);
    v1.add(new DomainAndVersion(domain1, 1));

    // V2
    versionsMap2.put(domain1, 2);
    v2.add(new DomainAndVersion(domain1, 2));

    // V3
    versionsMap3.put(domain1, 2);
    versionsMap3.put(domain2, 1);
    v3.add(new DomainAndVersion(domain1, 2));
    v3.add(new DomainAndVersion(domain2, 1));
  }

  private void setUpRing(MockRingLocal ring,
                         Set<DomainAndVersion> currentVersions,
                         Set<DomainAndVersion> assignedVersions,
                         HostState hostState) throws IOException {
    if (assignedVersions != null) {
      partitionAssigner.prepare(ring, assignedVersions, rg.getRingGroupConductorMode());
    }
    for (Host host : ring.getHosts()) {
      if (assignedVersions != null) {
        partitionAssigner.assign(host);
      }
      host.setState(hostState);
      if (currentVersions != null) {
        ((MockHostLocal)host).setCurrentVersion(currentVersions);
      }
    }
  }

  private boolean isAssigned(Ring ring, Host host, Set<DomainAndVersion> domainVersions) throws IOException {
    partitionAssigner.prepare(ring, domainVersions, rg.getRingGroupConductorMode());
    return partitionAssigner.isAssigned(host);
  }

  @Test
  public void testIsFullyServing() throws IOException {
    RingGroupUpdateTransitionFunctionImpl transitionFunction = new RingGroupUpdateTransitionFunctionImpl(
        null,
        1,
        2,
        0,
        0,
        0,
        null
    );

    setUpRing(r0, v1, v1, HostState.IDLE);
    assertFalse(transitionFunction.isFullyServing(r0h0, true));
    assertFalse(transitionFunction.isFullyServing(r0h1, true));

    r0h0.setState(HostState.SERVING);
    assertFalse(transitionFunction.isFullyServing(r0h0, true));
    assertFalse(transitionFunction.isFullyServing(r0h1, true));

    r0h1.setState(HostState.SERVING);

    assertTrue(transitionFunction.isFullyServing(r0h0, true));
    assertFalse(transitionFunction.isFullyServing(r0h1, true));
    assertTrue(transitionFunction.isFullyServing(r0h1, true));

    r0h0.enqueueCommand(HostCommand.GO_TO_IDLE);
    assertFalse(transitionFunction.isFullyServing(r0h0, true));

    r0h0.setCurrentCommand(HostCommand.GO_TO_IDLE);
    r0h0.clearCommandQueue();
    assertFalse(transitionFunction.isFullyServing(r0h0, true));
  }

  @Test
  public void testNothingToDo() throws IOException {
    domainGroup.setDomainVersions(versionsMap1);

    setUpRing(r0, v1, v1, HostState.SERVING);
    setUpRing(r1, v1, v1, HostState.SERVING);
    setUpRing(r2, v1, v1, HostState.SERVING);

    testTransitionFunction.manageTransitions(null, rg);

    // No commands should have been issued
    assertNull(r0h0.getAndClearLastEnqueuedCommand());
    assertNull(r0h1.getAndClearLastEnqueuedCommand());
    assertNull(r1h0.getAndClearLastEnqueuedCommand());
    assertNull(r1h1.getAndClearLastEnqueuedCommand());
    assertNull(r2h0.getAndClearLastEnqueuedCommand());
    assertNull(r2h1.getAndClearLastEnqueuedCommand());
  }

  @Test
  public void testKickstartAllRings() throws IOException {
    domainGroup.setDomainVersions(versionsMap1);

    setUpRing(r0, null, null, HostState.IDLE);
    setUpRing(r1, null, null, HostState.IDLE);
    setUpRing(r2, null, null, HostState.IDLE);

    // Nothing should be assigned
    assertFalse(isAssigned(r0, r0h0, v1));
    assertFalse(isAssigned(r0, r0h1, v1));
    assertFalse(isAssigned(r1, r1h0, v1));
    assertFalse(isAssigned(r1, r1h1, v1));
    assertFalse(isAssigned(r2, r2h0, v1));
    assertFalse(isAssigned(r2, r2h1, v1));

    testTransitionFunction.manageTransitions(null, rg);

    // All rings should have been assigned
    assertTrue(isAssigned(r0, r0h0, v1));
    assertTrue(isAssigned(r0, r0h1, v1));
    assertTrue(isAssigned(r1, r1h0, v1));
    assertTrue(isAssigned(r1, r1h1, v1));
    assertTrue(isAssigned(r2, r2h0, v1));
    assertTrue(isAssigned(r2, r2h1, v1));

    // No commands should have been issued
    assertNull(r0h0.getAndClearLastEnqueuedCommand());
    assertNull(r0h1.getAndClearLastEnqueuedCommand());
    assertNull(r1h0.getAndClearLastEnqueuedCommand());
    assertNull(r1h1.getAndClearLastEnqueuedCommand());
    assertNull(r2h0.getAndClearLastEnqueuedCommand());
    assertNull(r2h1.getAndClearLastEnqueuedCommand());

    testTransitionFunction.manageTransitions(null, rg);

    // All hosts should have received execute update
    assertEquals(HostCommand.EXECUTE_UPDATE, r0h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.EXECUTE_UPDATE, r0h1.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.EXECUTE_UPDATE, r1h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.EXECUTE_UPDATE, r1h1.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.EXECUTE_UPDATE, r2h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.EXECUTE_UPDATE, r2h1.getAndClearLastEnqueuedCommand());
  }

  @Test
  public void testTakesDownFirstRingForAssignmentWhenStartingUpdate() throws IOException {
    domainGroup.setDomainVersions(versionsMap3);

    setUpRing(r0, v2, v2, HostState.SERVING);
    setUpRing(r1, v2, v2, HostState.SERVING);
    setUpRing(r2, v2, v2, HostState.SERVING);

    testTransitionFunction.manageTransitions(null, rg);

    // All serving hosts in r0 should have received go to idle
    assertEquals(HostCommand.GO_TO_IDLE, r0h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.GO_TO_IDLE, r0h1.getAndClearLastEnqueuedCommand());

    // No commands should have been issued to other rings
    assertNull(r1h0.getAndClearLastEnqueuedCommand());
    assertNull(r1h1.getAndClearLastEnqueuedCommand());
    assertNull(r2h0.getAndClearLastEnqueuedCommand());
    assertNull(r2h1.getAndClearLastEnqueuedCommand());

    // Nothing should be assigned
    assertFalse(isAssigned(r0, r0h0, v3));
    assertFalse(isAssigned(r0, r0h1, v3));
    assertFalse(isAssigned(r1, r1h0, v3));
    assertFalse(isAssigned(r1, r1h1, v3));
    assertFalse(isAssigned(r2, r2h0, v3));
    assertFalse(isAssigned(r2, r2h1, v3));

    // Make hosts idle
    r0h0.nextCommand();
    r0h1.nextCommand();
    r0h0.setState(HostState.IDLE);
    r0h1.setState(HostState.IDLE);

    testTransitionFunction.manageTransitions(null, rg);

    // Ring one should have been assigned
    assertTrue(isAssigned(r0, r0h0, v3));
    assertTrue(isAssigned(r0, r0h1, v3));
    assertFalse(isAssigned(r1, r1h0, v3));
    assertFalse(isAssigned(r1, r1h1, v3));
    assertFalse(isAssigned(r2, r2h0, v3));
    assertFalse(isAssigned(r2, r2h1, v3));
  }

  @Test
  public void testTakesDownFirstRingForUpdateWhenStartingUpdate() throws IOException {
    domainGroup.setDomainVersions(versionsMap2);

    setUpRing(r0, v1, v2, HostState.SERVING);
    setUpRing(r1, v1, v2, HostState.SERVING);
    setUpRing(r2, v1, v2, HostState.SERVING);

    testTransitionFunction.manageTransitions(null, rg);

    // All serving hosts in r0 should have received go to idle
    assertEquals(HostCommand.GO_TO_IDLE, r0h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.GO_TO_IDLE, r0h1.getAndClearLastEnqueuedCommand());

    // No commands should have been issued to other rings
    assertNull(r1h0.getAndClearLastEnqueuedCommand());
    assertNull(r1h1.getAndClearLastEnqueuedCommand());
    assertNull(r2h0.getAndClearLastEnqueuedCommand());
    assertNull(r2h1.getAndClearLastEnqueuedCommand());

    // Make hosts idle
    r0h0.nextCommand();
    r0h1.nextCommand();
    r0h0.setState(HostState.IDLE);
    r0h1.setState(HostState.IDLE);

    testTransitionFunction.manageTransitions(null, rg);

    // All idle hosts in r0 should have received execute update
    assertEquals(HostCommand.EXECUTE_UPDATE, r0h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.EXECUTE_UPDATE, r0h1.getAndClearLastEnqueuedCommand());

    // No commands should have been issued to other rings
    assertNull(r1h0.getAndClearLastEnqueuedCommand());
    assertNull(r1h1.getAndClearLastEnqueuedCommand());
    assertNull(r2h0.getAndClearLastEnqueuedCommand());
    assertNull(r2h1.getAndClearLastEnqueuedCommand());
  }

  @Test
  public void testAssignWhenOneHostIsServing() throws IOException {
    domainGroup.setDomainVersions(versionsMap3);

    setUpRing(r0, v1, v1, HostState.IDLE);
    r0h1.setState(HostState.SERVING);
    setUpRing(r1, v1, v1, HostState.SERVING);
    setUpRing(r2, v1, v1, HostState.SERVING);

    testTransitionFunction.manageTransitions(null, rg);

    // v2 should have been assigned to r0h0 but not r0h1
    assertTrue(isAssigned(r0, r0h0, v3));
    assertFalse(isAssigned(r0, r0h1, v3));

    // No commands should have been issued to rings, except to r0h1
    assertNull(r0h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.GO_TO_IDLE, r0h1.getAndClearLastEnqueuedCommand());
    assertNull(r1h0.getAndClearLastEnqueuedCommand());
    assertNull(r1h1.getAndClearLastEnqueuedCommand());
    assertNull(r2h0.getAndClearLastEnqueuedCommand());
    assertNull(r2h1.getAndClearLastEnqueuedCommand());

    // Make host idle
    r0h1.nextCommand();
    r0h1.setState(HostState.IDLE);

    testTransitionFunction.manageTransitions(null, rg);

    // v2 should have been assigned to r0h1
    assertTrue(isAssigned(r0, r0h0, v3));
    assertTrue(isAssigned(r0, r0h1, v3));

    // r0h0 should be updating
    assertEquals(HostCommand.EXECUTE_UPDATE, r0h0.getAndClearLastEnqueuedCommand());
    assertNull(r0h1.getAndClearLastEnqueuedCommand());
    assertNull(r1h0.getAndClearLastEnqueuedCommand());
    assertNull(r1h1.getAndClearLastEnqueuedCommand());
    assertNull(r2h0.getAndClearLastEnqueuedCommand());
    assertNull(r2h1.getAndClearLastEnqueuedCommand());
  }

  @Test
  public void testAssignWhenOneHostIsUpdating() throws IOException {
    domainGroup.setDomainVersions(versionsMap3);

    setUpRing(r0, v1, v1, HostState.IDLE);
    r0h1.setState(HostState.UPDATING);
    setUpRing(r1, v1, v1, HostState.SERVING);
    setUpRing(r2, v1, v1, HostState.SERVING);

    testTransitionFunction.manageTransitions(null, rg);

    // v2 should have been assigned to r0h0 but not r0h1
    assertTrue(isAssigned(r0, r0h0, v3));
    assertFalse(isAssigned(r0, r0h1, v3));

    // No commands should have been issued to rings
    assertNull(r0h0.getAndClearLastEnqueuedCommand());
    assertNull(r0h1.getAndClearLastEnqueuedCommand());
    assertNull(r1h0.getAndClearLastEnqueuedCommand());
    assertNull(r1h1.getAndClearLastEnqueuedCommand());
    assertNull(r2h0.getAndClearLastEnqueuedCommand());
    assertNull(r2h1.getAndClearLastEnqueuedCommand());
  }

  @Test
  public void testAssignIdleRing() throws IOException {
    domainGroup.setDomainVersions(versionsMap3);

    setUpRing(r0, v1, v1, HostState.IDLE);
    setUpRing(r1, v1, v1, HostState.SERVING);
    setUpRing(r2, v1, v1, HostState.SERVING);

    testTransitionFunction.manageTransitions(null, rg);

    assertTrue(isAssigned(r0, r0h0, v3));
    assertTrue(isAssigned(r0, r0h1, v3));
    assertFalse(isAssigned(r1, r1h0, v3));
    assertFalse(isAssigned(r1, r1h1, v3));
    assertFalse(isAssigned(r2, r2h0, v3));
    assertFalse(isAssigned(r2, r2h1, v3));

    // No commands should have been issued to rings
    assertNull(r0h0.getAndClearLastEnqueuedCommand());
    assertNull(r0h1.getAndClearLastEnqueuedCommand());
    assertNull(r1h0.getAndClearLastEnqueuedCommand());
    assertNull(r1h1.getAndClearLastEnqueuedCommand());
    assertNull(r2h0.getAndClearLastEnqueuedCommand());
    assertNull(r2h1.getAndClearLastEnqueuedCommand());
  }

  @Test
  public void testTakeDownModifiedRing() throws IOException {
    domainGroup.setDomainVersions(versionsMap1);

    setUpRing(r0, v1, null, HostState.SERVING);
    setUpRing(r1, v1, v1, HostState.SERVING);
    setUpRing(r2, v1, v1, HostState.SERVING);

    testTransitionFunction.manageTransitions(null, rg);

    // Hosts of r0 should have received go to idle
    assertEquals(HostCommand.GO_TO_IDLE, r0h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.GO_TO_IDLE, r0h1.getAndClearLastEnqueuedCommand());

    // No commands should have been issued to other rings
    assertNull(r1h0.getAndClearLastEnqueuedCommand());
    assertNull(r1h1.getAndClearLastEnqueuedCommand());
    assertNull(r2h0.getAndClearLastEnqueuedCommand());
    assertNull(r2h1.getAndClearLastEnqueuedCommand());
  }

  @Test
  public void testAssignModifiedRing() throws IOException {
    domainGroup.setDomainVersions(versionsMap1);

    setUpRing(r0, v1, null, HostState.IDLE);
    setUpRing(r1, v1, v1, HostState.SERVING);
    setUpRing(r2, v1, v1, HostState.SERVING);

    testTransitionFunction.manageTransitions(null, rg);

    // v1 should have been assigned to r0
    assertTrue(isAssigned(r0, r0h0, v1));
    assertTrue(isAssigned(r0, r0h1, v1));

    // No commands should have been issued to rings
    assertNull(r0h0.getAndClearLastEnqueuedCommand());
    assertNull(r0h1.getAndClearLastEnqueuedCommand());
    assertNull(r1h0.getAndClearLastEnqueuedCommand());
    assertNull(r1h1.getAndClearLastEnqueuedCommand());
    assertNull(r2h0.getAndClearLastEnqueuedCommand());
    assertNull(r2h1.getAndClearLastEnqueuedCommand());
  }

  @Test
  public void testReassignProactivelyWhenHostsAreOffline() throws IOException {
    domainGroup.setDomainVersions(versionsMap1);

    setUpRing(r0, v1, v1, HostState.SERVING);
    setUpRing(r1, v1, v1, HostState.SERVING);
    setUpRing(r2, v1, v1, HostState.SERVING);

    assertTrue(isAssigned(r0, r0h0, v1));
    assertTrue(isAssigned(r0, r0h1, v1));

    r0h0.setState(HostState.OFFLINE);

    assertTrue(isAssigned(r0, r0h0, v1));
    assertTrue(isAssigned(r0, r0h1, v1));

    // Switch to proactive
    rg.setRingGroupConductorMode(RingGroupConductorMode.PROACTIVE);

    assertFalse(isAssigned(r0, r0h0, v1));
    assertFalse(isAssigned(r0, r0h1, v1));

    testTransitionFunction.manageTransitions(null, rg);

    // r0h1 should be going idle for assignment
    assertNull(r0h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.GO_TO_IDLE, r0h1.getAndClearLastEnqueuedCommand());

    r0h1.setState(HostState.IDLE);

    testTransitionFunction.manageTransitions(null, rg);

    assertFalse(isAssigned(r0, r0h0, v1));
    assertTrue(isAssigned(r0, r0h1, v1));

    testTransitionFunction.manageTransitions(null, rg);

    // r0h1 should be executing update
    assertNull(r0h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.EXECUTE_UPDATE, r0h1.getAndClearLastEnqueuedCommand());
  }

  @Test
  public void testExecuteUpdateWhenAssignedAndIdle() throws IOException {
    domainGroup.setDomainVersions(versionsMap2);

    setUpRing(r0, v1, v2, HostState.IDLE);
    setUpRing(r1, v1, v1, HostState.SERVING);
    setUpRing(r2, v1, v1, HostState.SERVING);

    testTransitionFunction.manageTransitions(null, rg);

    // Hosts of r0 should have received execute update
    assertEquals(HostCommand.EXECUTE_UPDATE, r0h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.EXECUTE_UPDATE, r0h1.getAndClearLastEnqueuedCommand());

    // No commands should have been issued to rings
    assertNull(r1h0.getAndClearLastEnqueuedCommand());
    assertNull(r1h1.getAndClearLastEnqueuedCommand());
    assertNull(r2h0.getAndClearLastEnqueuedCommand());
    assertNull(r2h1.getAndClearLastEnqueuedCommand());
  }

  @Test
  public void testProactivelyServeDateWhenHostUpdated() throws IOException {
    domainGroup.setDomainVersions(versionsMap2);

    setUpRing(r0, v1, v2, HostState.UPDATING);
    setUpRing(r1, v1, v2, HostState.SERVING);
    setUpRing(r2, v1, v2, HostState.SERVING);

    r0h0.setCurrentVersion(v2);
    r0h0.setState(HostState.IDLE);

    testTransitionFunction.manageTransitions(null, rg);

    // r0h0 should have received serve data
    assertEquals(HostCommand.SERVE_DATA, r0h0.getAndClearLastEnqueuedCommand());

    // No commands should have been issued to rings
    assertNull(r0h1.getAndClearLastEnqueuedCommand());
    assertNull(r1h0.getAndClearLastEnqueuedCommand());
    assertNull(r1h1.getAndClearLastEnqueuedCommand());
    assertNull(r2h0.getAndClearLastEnqueuedCommand());
    assertNull(r2h1.getAndClearLastEnqueuedCommand());
  }

  @Test
  public void testServeDataWhenUpdated() throws IOException {
    domainGroup.setDomainVersions(versionsMap2);

    setUpRing(r0, v2, v2, HostState.IDLE);
    setUpRing(r1, v1, v1, HostState.SERVING);
    setUpRing(r2, v1, v1, HostState.SERVING);

    testTransitionFunction.manageTransitions(null, rg);

    // Hosts of r0 should have received serve data
    assertEquals(HostCommand.SERVE_DATA, r0h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.SERVE_DATA, r0h1.getAndClearLastEnqueuedCommand());

    // No commands should have been issued to rings
    assertNull(r1h0.getAndClearLastEnqueuedCommand());
    assertNull(r1h1.getAndClearLastEnqueuedCommand());
    assertNull(r2h0.getAndClearLastEnqueuedCommand());
    assertNull(r2h1.getAndClearLastEnqueuedCommand());
  }

  @Test
  public void testTakeDownSecondRingWhenFirstIsUpdated() throws IOException {
    domainGroup.setDomainVersions(versionsMap2);

    setUpRing(r0, v2, v2, HostState.SERVING);
    setUpRing(r1, v1, v2, HostState.SERVING);
    setUpRing(r2, v1, v2, HostState.SERVING);

    testTransitionFunction.manageTransitions(null, rg);

    // Hosts of r1 should have received go to idle
    assertEquals(HostCommand.GO_TO_IDLE, r1h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.GO_TO_IDLE, r1h1.getAndClearLastEnqueuedCommand());

    // No commands should have been issued to rings
    assertNull(r0h0.getAndClearLastEnqueuedCommand());
    assertNull(r0h1.getAndClearLastEnqueuedCommand());
    assertNull(r2h0.getAndClearLastEnqueuedCommand());
    assertNull(r2h1.getAndClearLastEnqueuedCommand());
  }

  @Test
  public void testServeDataWhenNotEnoughRingsAreFullyServing() throws IOException {
    domainGroup.setDomainVersions(versionsMap2);

    setUpRing(r0, v1, v2, HostState.SERVING);
    setUpRing(r1, v1, v1, HostState.IDLE);
    setUpRing(r2, v1, v2, HostState.IDLE);

    testTransitionFunction.manageTransitions(null, rg);

    // No commands should have been issued to r0
    assertNull(r0h0.getAndClearLastEnqueuedCommand());
    assertNull(r0h1.getAndClearLastEnqueuedCommand());

    // Other hosts should have received serve data
    assertEquals(HostCommand.SERVE_DATA, r1h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.SERVE_DATA, r1h1.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.SERVE_DATA, r2h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.SERVE_DATA, r2h1.getAndClearLastEnqueuedCommand());
  }

  @Test
  public void testUpdateMultipleRingsWhenEnoughReplicasAreServing() throws IOException {
    domainGroup.setDomainVersions(versionsMap2);

    // r0: h0 SERVING, h1 UPDATING
    // r1: h0 SERVING, h1 SERVING
    // r2: h0 SERVING, h1 SERVING

    setUpRing(r0, v1, v2, HostState.SERVING);
    // Make r0h0 up to date
    partitionAssigner.prepare(r0, v2, rg.getRingGroupConductorMode());
    partitionAssigner.assign(r0h0);
    r0h0.setCurrentVersion(v2);
    // r0h1 is still updating
    r0h1.setState(HostState.UPDATING);
    setUpRing(r1, v1, v1, HostState.SERVING);
    setUpRing(r2, v1, v2, HostState.SERVING);

    testTransitionFunction.manageTransitions(null, rg);

    // No commands should have been issued to offline hosts and up to date hosts
    assertNull(r0h0.getAndClearLastEnqueuedCommand());
    assertNull(r0h1.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.GO_TO_IDLE, r1h0.getAndClearLastEnqueuedCommand());
    assertNull(r1h1.getAndClearLastEnqueuedCommand());
    assertNull(r2h0.getAndClearLastEnqueuedCommand());
    assertNull(r2h1.getAndClearLastEnqueuedCommand());
  }

  //  2 - 2 : doesn't update both in either AZ

  @Test
  public void testSingleUpdateSameBucket() throws IOException {
    domainGroup.setDomainVersions(versionsMap2);

    r0h0.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));
    r0h1.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));

    r1h0.setEnvironmentFlags(Collections.singletonMap("AZ", "b"));
    r1h1.setEnvironmentFlags(Collections.singletonMap("AZ", "b"));

    r2h0.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));
    r2h1.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));

    //  all rings need to be updated
    setUpRing(r0, v1, v2, HostState.SERVING);
    setUpRing(r1, v1, v2, HostState.SERVING);
    setUpRing(r2, v1, v2, HostState.SERVING);

    //  require at least 1 replica up per AZ.
    testTransitionFunction = new RingGroupUpdateTransitionFunctionImpl(partitionAssigner,
        0,
        1,
        0,
        1,
        0,
        "AZ"
    );
    testTransitionFunction.manageTransitions(null, rg);

    //  r0 can go idle
    assertEquals(HostCommand.GO_TO_IDLE, r0h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.GO_TO_IDLE, r0h1.getAndClearLastEnqueuedCommand());

    //  r1 can't
    assertEquals(null, r1h0.getAndClearLastEnqueuedCommand());
    assertEquals(null, r1h1.getAndClearLastEnqueuedCommand());

    //  r2 can't
    assertEquals(null, r2h0.getAndClearLastEnqueuedCommand());
    assertEquals(null, r2h1.getAndClearLastEnqueuedCommand());

  }

  @Test
  public void testUpdateWithNoMin() throws IOException {
    domainGroup.setDomainVersions(versionsMap2);

    r0h0.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));
    r0h1.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));

    r1h0.setEnvironmentFlags(Collections.singletonMap("AZ", "b"));
    r1h1.setEnvironmentFlags(Collections.singletonMap("AZ", "b"));

    r2h0.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));
    r2h1.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));

    //  all rings need to be updated
    setUpRing(r0, v1, v2, HostState.SERVING);
    setUpRing(r1, v1, v2, HostState.SERVING);
    setUpRing(r2, v1, v2, HostState.SERVING);

    //  no minimum number of replicas per zone
    testTransitionFunction = new RingGroupUpdateTransitionFunctionImpl(partitionAssigner,
        0, 1, 0, 0, 0, "AZ");
    testTransitionFunction.manageTransitions(null, rg);

    //  r0 can go idle
    assertEquals(HostCommand.GO_TO_IDLE, r0h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.GO_TO_IDLE, r0h1.getAndClearLastEnqueuedCommand());

    //  r1 can as well, since 0 min
    assertEquals(HostCommand.GO_TO_IDLE, r1h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.GO_TO_IDLE, r1h1.getAndClearLastEnqueuedCommand());

    //  r2 can't, limited by global 1 min replica.
    assertEquals(null, r2h0.getAndClearLastEnqueuedCommand());
    assertEquals(null, r2h1.getAndClearLastEnqueuedCommand());

  }

  @Test
  public void testUpdateWithMinPercent() throws IOException {

    domainGroup.setDomainVersions(versionsMap2);

    r0h0.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));
    r0h1.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));

    r1h0.setEnvironmentFlags(Collections.singletonMap("AZ", "b"));
    r1h1.setEnvironmentFlags(Collections.singletonMap("AZ", "b"));

    r2h0.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));
    r2h1.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));

    //  all rings need to be updated
    setUpRing(r0, v1, v2, HostState.SERVING);
    setUpRing(r1, v1, v2, HostState.SERVING);
    setUpRing(r2, v1, v2, HostState.SERVING);


    testTransitionFunction = new RingGroupUpdateTransitionFunctionImpl(partitionAssigner,
        0,
        0,
        .5,
        0,
        .5,
        "AZ");
    testTransitionFunction.manageTransitions(null, rg);

    //  r0 can go idle
    assertEquals(HostCommand.GO_TO_IDLE, r0h0.getAndClearLastEnqueuedCommand());
    assertEquals(HostCommand.GO_TO_IDLE, r0h1.getAndClearLastEnqueuedCommand());

    //  r2 can't, limited by .5 min replica.
    assertEquals(null, r1h0.getAndClearLastEnqueuedCommand());
    assertEquals(null, r1h1.getAndClearLastEnqueuedCommand());

    //  r2 can't, limited by .5 min replica.
    assertEquals(null, r2h0.getAndClearLastEnqueuedCommand());
    assertEquals(null, r2h1.getAndClearLastEnqueuedCommand());


  }

  @Test
  public void testUpdateWithHighPercent() throws IOException {

    domainGroup.setDomainVersions(versionsMap2);

    r0h0.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));
    r0h1.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));

    r2h0.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));
    r2h1.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));

    //  all rings need to be updated
    setUpRing(r0, v1, v2, HostState.SERVING);
    setUpRing(r2, v1, v2, HostState.SERVING);

    testTransitionFunction = new RingGroupUpdateTransitionFunctionImpl(partitionAssigner,
        0,
        0,
        .6,
        0,
        0,
        "AZ");
    testTransitionFunction.manageTransitions(null, rg);

    //  r0 can go idle
    assertEquals(null, r0h0.getAndClearLastEnqueuedCommand());
    assertEquals(null, r0h1.getAndClearLastEnqueuedCommand());

    //  r2 can't, limited by .6 min replica.
    assertEquals(null, r1h0.getAndClearLastEnqueuedCommand());
    assertEquals(null, r1h1.getAndClearLastEnqueuedCommand());


  }

  @Test
  public void testUpdateWithHighBucketPercent() throws IOException {

    domainGroup.setDomainVersions(versionsMap2);

    r0h0.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));
    r0h1.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));

    r2h0.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));
    r2h1.setEnvironmentFlags(Collections.singletonMap("AZ", "a"));

    //  all rings need to be updated
    setUpRing(r0, v1, v2, HostState.SERVING);
    setUpRing(r2, v1, v2, HostState.SERVING);

    testTransitionFunction = new RingGroupUpdateTransitionFunctionImpl(partitionAssigner,
        0,
        0,
        0,
        0,
        .6,
        "AZ");
    testTransitionFunction.manageTransitions(null, rg);

    //  r0 can go idle
    assertEquals(null, r0h0.getAndClearLastEnqueuedCommand());
    assertEquals(null, r0h1.getAndClearLastEnqueuedCommand());

    //  r2 can't, limited by .6 min replica.
    assertEquals(null, r1h0.getAndClearLastEnqueuedCommand());
    assertEquals(null, r1h1.getAndClearLastEnqueuedCommand());


  }



}
