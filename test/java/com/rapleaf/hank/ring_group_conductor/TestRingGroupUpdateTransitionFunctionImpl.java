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
package com.rapleaf.hank.ring_group_conductor;

import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainGroup;
import junit.framework.TestCase;

import java.util.*;

public class TestRingGroupUpdateTransitionFunctionImpl extends TestCase {

  private class MockRingLocal extends MockRing {

    protected DomainGroupVersion currentVersion;


    public MockRingLocal(int number,
                         DomainGroupVersion currentVersion,
                         PartitionServerAddress... hosts) {
      super(new LinkedHashSet<PartitionServerAddress>(Arrays.asList(hosts)), null, number);
      this.currentVersion = currentVersion;
    }
  }

  private class MockRingGroupLocal extends MockRingGroup {

    public MockRingGroupLocal(int targetVersion, Ring... rings) {
      super(domainGroup, "myRingGroup", new LinkedHashSet<Ring>(Arrays.asList(rings)), targetVersion);
    }
  }

  private static Domain domain1 = new MockDomain("domain1");

  private static PartitionServerAddress address1 = new PartitionServerAddress("localhost", 1);
  private static PartitionServerAddress address2 = new PartitionServerAddress("localhost", 2);

  private static DomainGroupVersion host1CurrentVersion = null;
  private static DomainGroupVersion host2CurrentVersion = null;

  private static DomainGroup domainGroup = new MockDomainGroup("myDomainGroup") {

    @Override
    public DomainGroupVersion getVersionByNumber(int versionNumber) {
      switch (versionNumber) {
        case 1:
          return v1;
        case 2:
          return v2;
        case 3:
          return v3;
        default:
          throw new RuntimeException("Unknown version: " + versionNumber);
      }
    }
  };

  private static DomainGroupVersion v1 =
      new MockDomainGroupVersion(Collections.<DomainGroupVersionDomainVersion>emptySet(), domainGroup, 1);
  private static DomainGroupVersion v2 =
      new MockDomainGroupVersion(Collections.<DomainGroupVersionDomainVersion>emptySet(), domainGroup, 2);
  private static Set<DomainGroupVersionDomainVersion> domainVersions = new HashSet<DomainGroupVersionDomainVersion>() {{
    add(new MockDomainGroupVersionDomainVersion(domain1, 0));
  }};
  private static DomainGroupVersion v3 = new MockDomainGroupVersion(domainVersions, domainGroup, 2);

  //TODO: write new tests

  /*
  public void testDownsFirstAvailableRing() throws Exception {
    MockRingLocal r1 = new MockRingLocal(1, RingState.OPEN, v1, address1);
    MockRingLocal r2 = new MockRingLocal(2, RingState.OPEN, v1, address2);
    MockRingGroupLocal rg = new MockRingGroupLocal(2, r1, r2);
    getFunc().manageTransitions(rg);

    assertTrue("r1 should have been taken down", r1.isAllCommanded(HostCommand.GO_TO_IDLE));
    assertEquals(RingState.CLOSING, r1.getState());
    assertFalse("r2 should not have been taken down", r2.isAllCommanded(HostCommand.GO_TO_IDLE));
    assertEquals(RingState.OPEN, r2.getState());
  }

  private RingGroupUpdateTransitionFunction getFunc() {
    return new RingGroupUpdateTransitionFunctionImpl() {

      @Override
      protected boolean isUpToDate(Ring ring, DomainGroupVersion domainGroupVersion) {
        MockRingLocal mockRing = (MockRingLocal) ring;
        return mockRing.currentVersion != null &&
            mockRing.currentVersion.equals(domainGroupVersion);
      }

      @Override
      protected boolean isUpToDate(Host host, DomainGroupVersion domainGroupVersion) {
        if (host.getAddress().equals(address1)) {
          return domainGroupVersion.equals(host1CurrentVersion);
        } else if (host.getAddress().equals(address2)) {
          return domainGroupVersion.equals(host2CurrentVersion);
        } else {
          throw new RuntimeException("Unknown host: " + host);
        }
      }
    };
  }

  public void testDownsOnlyNotYetUpdatedRing() throws Exception {
    // this ring is fully updated
    MockRingLocal r1 = new MockRingLocal(1, RingState.OPEN, v2, address1);
    MockRingLocal r2 = new MockRingLocal(2, RingState.OPEN, v1, address2);
    MockRingGroupLocal rg = new MockRingGroupLocal(2, r1, r2);
    getFunc().manageTransitions(rg);

    assertFalse("r1 should not have been taken down", r1.isAllCommanded(HostCommand.GO_TO_IDLE));
    assertEquals(RingState.OPEN, r1.getState());
    assertTrue("r2 should have been taken down", r2.isAllCommanded(HostCommand.GO_TO_IDLE));
    assertEquals(RingState.CLOSING, r2.getState());
  }

  public void testDownsNothingIfSomethingIsAlreadyDown() throws Exception {
    for (RingState s : EnumSet.of(RingState.CLOSING, RingState.OPENING, RingState.UPDATING, RingState.CLOSED)) {
      MockRingLocal r1 = new MockRingLocal(1, s, v1, address1);
      MockRingLocal r2 = new MockRingLocal(2, RingState.OPEN, v1, address2);
      MockRingGroupLocal rg = new MockRingGroupLocal(2, r1, r2);
      getFunc().manageTransitions(rg);

      assertEquals("r2 should still be up", RingState.OPEN, r2.getState());
    }
  }

  public void testDownToUpdating() throws Exception {
    MockRingLocal r1 = new MockRingLocal(1, RingState.CLOSED, v1, address1);
    MockRingGroupLocal rg = new MockRingGroupLocal(2, r1);
    getFunc().manageTransitions(rg);

    assertTrue("r1 should have been set to updating", r1.isAllCommanded(HostCommand.EXECUTE_UPDATE));
    assertEquals(RingState.UPDATING, r1.getState());
  }

  public void testUpdatingToComingUp() throws Exception {
    MockRingLocal r1 = new MockRingLocal(1, RingState.UPDATING, v2, address1);
    MockRingGroupLocal rg = new MockRingGroupLocal(2, r1);
    getFunc().manageTransitions(rg);

    assertTrue("r1 should have been set to starting", r1.isAllCommanded(HostCommand.SERVE_DATA));
    assertEquals(RingState.OPENING, r1.getState());
  }

  public void testDoesntLeaveUpdatingWhenThereAreStillHostsUpdating() throws Exception {
    MockRingLocal r1 = new MockRingLocal(1, RingState.UPDATING, v1, address1);
    r1.getHostByAddress(address1).setState(HostState.UPDATING);
    MockRingGroupLocal rg = new MockRingGroupLocal(2, r1);
    getFunc().manageTransitions(rg);

    assertFalse("r1 should not have been set to starting", r1.isAllCommanded(HostCommand.SERVE_DATA));
    assertEquals(RingState.UPDATING, r1.getState());
  }

  // this case will only occur when the Ring Group Conductor has died or something.
  public void testUpdatedToComingUp() throws Exception {
    MockRingLocal r1 = new MockRingLocal(1, RingState.UPDATED, v1, address1);
    MockRingGroupLocal rg = new MockRingGroupLocal(2, r1);
    getFunc().manageTransitions(rg);

    assertTrue("r1's hosts should be commanded to start", r1.isAllCommanded(HostCommand.SERVE_DATA));
    assertEquals(RingState.OPENING, r1.getState());
  }

  public void testFailedUpdateNotComingUp() throws Exception {
    MockRingLocal r1 = new MockRingLocal(1, RingState.UPDATING, v1, address1);
    MockRingGroupLocal rg = new MockRingGroupLocal(2, r1);
    MockHost host = ((MockHost) r1.getHostByAddress(address1));
    host1CurrentVersion = v1;
    getFunc().manageTransitions(rg);

    assertEquals(RingState.UPDATING, r1.getState());
    assertFalse("r1's hosts should not be commanded to start", r1.isAllCommanded(HostCommand.SERVE_DATA));
    assertEquals("Not up to date host should have been commanded to update",
        HostCommand.EXECUTE_UPDATE, host.getLastEnqueuedCommand());
  }

  public void testComingUpToUp() throws Exception {
    MockRingLocal r1 = new MockRingLocal(1, RingState.OPENING, v1, address1);
    r1.getHostByAddress(address1).setState(HostState.SERVING);
    MockRingGroupLocal rg = new MockRingGroupLocal(2, r1);
    getFunc().manageTransitions(rg);
    assertEquals(RingState.OPEN, r1.getState());
  }
  */
}
