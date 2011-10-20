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
import junit.framework.TestCase;

import java.util.*;

public class TestRingGroupUpdateTransitionFunctionImpl extends TestCase {
  private class MockRingLocal extends MockRing {
    private final int currentVersion;
    private Integer nextVersion;

    public boolean updateCompleteCalled;

    public MockRingLocal(int number,
                         RingState state,
                         int currentVersion,
                         Integer nextVersion,
                         PartitionServerAddress... hosts) {
      super(new LinkedHashSet<PartitionServerAddress>(Arrays.asList(hosts)), null, number, state);
      this.currentVersion = currentVersion;
      this.nextVersion = nextVersion;
    }

    @Override
    public Integer getUpdatingToVersionNumber() {
      return nextVersion;
    }

    @Override
    public Integer getVersionNumber() {
      return currentVersion;
    }

    @Override
    public void updateComplete() {
      this.updateCompleteCalled = true;
    }
  }

  private class MockRingGroupLocal extends MockRingGroup {
    private final int curVer;
    @SuppressWarnings("unused")
    private final int toVer;

    public MockRingGroupLocal(int curVer, int toVer, Ring... ringConfigs) {
      super(null, "myRingGroup", new LinkedHashSet<Ring>(Arrays.asList(ringConfigs)));
      this.curVer = curVer;
      this.toVer = toVer;
    }

    @Override
    public Integer getCurrentVersion() {
      return curVer;
    }

    @Override
    public boolean isUpdating() {
      return true;
    }
  }

  private static PartitionServerAddress address1 = new PartitionServerAddress("localhost", 1);
  private static PartitionServerAddress address2 = new PartitionServerAddress("localhost", 2);


  public void testDownsFirstAvailableRing() throws Exception {
    MockRingLocal r1 = new MockRingLocal(1, RingState.UP, 1, 2, address1);
    MockRingLocal r2 = new MockRingLocal(2, RingState.UP, 1, 2, address2);
    MockRingGroupLocal rg = new MockRingGroupLocal(1, 2, r1, r2);
    getFunc().manageTransitions(rg);

    assertTrue("r1 should have been taken down", r1.isAllCommanded(HostCommand.GO_TO_IDLE));
    assertEquals(RingState.GOING_DOWN, r1.getState());
    assertFalse("r2 should not have been taken down", r2.isAllCommanded(HostCommand.GO_TO_IDLE));
    assertEquals(RingState.UP, r2.getState());
  }

  private RingGroupUpdateTransitionFunction getFunc() {
    return new RingGroupUpdateTransitionFunctionImpl();
  }

  public void testDownsOnlyNotYetUpdatedRing() throws Exception {
    // this ring is fully updated
    MockRingLocal r1 = new MockRingLocal(1, RingState.UP, 2, null, address1);
    MockRingLocal r2 = new MockRingLocal(2, RingState.UP, 1, 2, address2);
    MockRingGroupLocal rg = new MockRingGroupLocal(1, 2, r1, r2);
    getFunc().manageTransitions(rg);

    assertFalse("r1 should not have been taken down", r1.isAllCommanded(HostCommand.GO_TO_IDLE));
    assertEquals(RingState.UP, r1.getState());
    assertTrue("r2 should have been taken down", r2.isAllCommanded(HostCommand.GO_TO_IDLE));
    assertEquals(RingState.GOING_DOWN, r2.getState());
  }

  public void testDownsNothingIfSomethingIsAlreadyDown() throws Exception {
    for (RingState s : EnumSet.of(RingState.GOING_DOWN, RingState.COMING_UP, RingState.UPDATING, RingState.DOWN)) {
      MockRingLocal r1 = new MockRingLocal(1, s, 1, 2, address1);
      MockRingLocal r2 = new MockRingLocal(2, RingState.UP, 1, 2, address2);
      MockRingGroupLocal rg = new MockRingGroupLocal(1, 2, r1, r2);
      getFunc().manageTransitions(rg);

      assertEquals("r2 should still be up", RingState.UP, r2.getState());
    }
  }

  public void testDownToUpdating() throws Exception {
    MockRingLocal r1 = new MockRingLocal(1, RingState.DOWN, 1, 2, address1);
    MockRingGroupLocal rg = new MockRingGroupLocal(1, 2, r1);
    getFunc().manageTransitions(rg);

    assertTrue("r1 should have been set to updating", r1.isAllCommanded(HostCommand.EXECUTE_UPDATE));
    assertEquals(RingState.UPDATING, r1.getState());
  }

  public void testUpdatingToComingUp() throws Exception {
    MockRingLocal r1 = new MockRingLocal(1, RingState.UPDATING, 1, 2, address1);
    MockRingGroupLocal rg = new MockRingGroupLocal(1, 2, r1);
    getFunc().manageTransitions(rg);

    assertTrue("r1 should have been set to starting", r1.isAllCommanded(HostCommand.SERVE_DATA));
    assertEquals(RingState.COMING_UP, r1.getState());
  }

  public void testDoesntLeaveUpdatingWhenThereAreStillHostsUpdating() throws Exception {
    MockRingLocal r1 = new MockRingLocal(1, RingState.UPDATING, 1, 2, address1);
    r1.getHostByAddress(address1).setState(HostState.UPDATING);
    MockRingGroupLocal rg = new MockRingGroupLocal(1, 2, r1);
    getFunc().manageTransitions(rg);

    assertFalse("r1 should not have been set to starting", r1.isAllCommanded(HostCommand.SERVE_DATA));
    assertEquals(RingState.UPDATING, r1.getState());
  }

  // this case will only occur when the Ring Group Conductor has died or something.
  public void testUpdatedToComingUp() throws Exception {
    MockRingLocal r1 = new MockRingLocal(1, RingState.UPDATED, 1, 2, address1);
    MockRingGroupLocal rg = new MockRingGroupLocal(1, 2, r1);
    getFunc().manageTransitions(rg);

    assertTrue("r1 should have been set to update complete", r1.updateCompleteCalled);
    assertTrue("r1's hosts should be commanded to start", r1.isAllCommanded(HostCommand.SERVE_DATA));
    assertEquals(RingState.COMING_UP, r1.getState());
  }

  public void testComingUpToUp() throws Exception {
    MockRingLocal r1 = new MockRingLocal(1, RingState.COMING_UP, 1, 2, address1);
    r1.getHostByAddress(address1).setState(HostState.SERVING);
    MockRingGroupLocal rg = new MockRingGroupLocal(1, 2, r1);
    getFunc().manageTransitions(rg);
    assertEquals(RingState.UP, r1.getState());
  }
}
