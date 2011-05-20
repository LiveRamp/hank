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
package com.rapleaf.hank.data_deployer;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Set;

import junit.framework.TestCase;

import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.MockRing;
import com.rapleaf.hank.coordinator.MockRingGroup;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingState;

public class TestRingGroupUpdateTransitionFunctionImpl extends TestCase {
  private class MRC extends MockRing {
    private final int curVer;
    private final int nextVer;

    public boolean updateCompleteCalled;

    public MRC(int number,
        RingState state,
        boolean updatePending,
        int curVer,
        int nextVer,
        PartDaemonAddress... hosts) {
      super(new LinkedHashSet<PartDaemonAddress>(Arrays.asList(hosts)), null, number, state);
      this.curVer = curVer;
      this.nextVer = nextVer;
    }

    @Override
    public Integer getUpdatingToVersionNumber() {
      return nextVer;
    }

    @Override
    public Integer getVersionNumber() {
      return curVer;
    }

    @Override
    public void updateComplete() {
      this.updateCompleteCalled = true;
    }

    @Override
    public Set<Host> getHostsInState(HostState state) {
      return Collections.EMPTY_SET;
    }

  }

  private class MRG extends MockRingGroup {
    private final int curVer;
    @SuppressWarnings("unused")
    private final int toVer;

    public MRG(int curVer, int toVer, Ring... ringConfigs) {
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

  public void testDownsFirstAvailableRing() throws Exception {
    MRC r1 = new MRC(1, RingState.UP, true, 1, 2);
    MRC r2 = new MRC(2, RingState.UP, true, 1, 2);
    MRG rg = new MRG(1, 2, r1, r2);
    getFunc().manageTransitions(rg);

    assertEquals("r1 should have been taken down", HostCommand.GO_TO_IDLE, r1.allCommanded);
    assertEquals(RingState.GOING_DOWN, r1.getState());
    assertNull("r2 should not have been taken down", r2.allCommanded);
    assertEquals(RingState.UP, r2.getState());
  }

  private RingGroupUpdateTransitionFunction getFunc() {
    return new RingGroupUpdateTransitionFunctionImpl();
  }

  public void testDownsOnlyAvailableRing() throws Exception {
    MRC r1 = new MRC(1, RingState.UP, false, 2, 2);
    MRC r2 = new MRC(2, RingState.UP, true, 1, 2);
    MRG rg = new MRG(1, 2, r1, r2);
    getFunc().manageTransitions(rg);

    assertNull("r1 should not have been taken down", r1.allCommanded);
    assertEquals(RingState.UP, r1.getState());
    assertEquals("r2 should have been taken down", HostCommand.GO_TO_IDLE, r2.allCommanded);
    assertEquals(RingState.GOING_DOWN, r2.getState());
  }

  public void testDownsNothingIfSomethingIsAlreadyDown() throws Exception {
    for (RingState s : EnumSet.of(RingState.GOING_DOWN, RingState.COMING_UP, RingState.UPDATING, RingState.DOWN)) {
      MRC r1 = new MRC(1, s, true, 1, 2);
      MRC r2 = new MRC(2, RingState.UP, true, 1, 2);
      MRG rg = new MRG(1, 2, r1, r2);
      getFunc().manageTransitions(rg);

      assertEquals("r2 should still be up", RingState.UP, r2.getState());
    }
  }

  public void testDownToUpdating() throws Exception {
    MRC r1 = new MRC(1, RingState.DOWN, true, 1, 2);
    MRG rg = new MRG(1, 2, r1);
    getFunc().manageTransitions(rg);

    assertEquals("r1 should have been set to updating", HostCommand.EXECUTE_UPDATE, r1.allCommanded);
    assertEquals(RingState.UPDATING, r1.getState());
  }

  public void testUpdatingToComingUp() throws Exception {
    MRC r1 = new MRC(1, RingState.UPDATING, true, 1, 2, new PartDaemonAddress("localhost", 1)) {
      @Override
      public Set<Host> getHostsInState(HostState state) {
        switch (state) {
          case OFFLINE:
            return Collections.EMPTY_SET;
          case UPDATING:
            return Collections.EMPTY_SET;
          default:
            throw new IllegalStateException(state.toString());
        }
      }
    };
    MRG rg = new MRG(1, 2, r1);
    getFunc().manageTransitions(rg);

    assertEquals("r1 should have been set to starting", HostCommand.SERVE_DATA, r1.allCommanded);
    assertEquals(RingState.COMING_UP, r1.getState());
  }

  public void testDoesntLeaveUpdatingWhenThereAreStillHostsUpdating() throws Exception {
    MRC r1 = new MRC(1, RingState.UPDATING, true, 1, 2, new PartDaemonAddress("localhost", 1)) {
      @Override
      public Set<Host> getHostsInState(HostState state) {
        switch (state) {
          case OFFLINE:
            return Collections.EMPTY_SET;
          case UPDATING:
            return getHosts();
          default:
            throw new IllegalStateException(state.toString());
        }
      }
    };
    MRG rg = new MRG(1, 2, r1);
    getFunc().manageTransitions(rg);

    assertNull("r1 should have been set to starting", r1.allCommanded);
    assertEquals(RingState.UPDATING, r1.getState());
  }

  // this case will only occur when the data deployer has died or something.
  public void testUpdatedToComingUp() throws Exception {
    MRC r1 = new MRC(1, RingState.UPDATED, true, 1, 2, new PartDaemonAddress("localhost", 1)) {
      @Override
      public Set<Host> getHostsInState(HostState state) {
        switch (state) {
          case OFFLINE:
            return Collections.EMPTY_SET;
          case UPDATING:
            return Collections.EMPTY_SET;
          default:
            throw new IllegalStateException(state.toString());
        }
      }
    };
    MRG rg = new MRG(1, 2, r1);
    getFunc().manageTransitions(rg);

    assertEquals("r1's hosts should be commanded to start", HostCommand.SERVE_DATA, r1.allCommanded);
    assertEquals(RingState.COMING_UP, r1.getState());
  }

  public void testComingUpToUp() throws Exception {
    MRC r1 = new MRC(1, RingState.COMING_UP, true, 1, 2, new PartDaemonAddress("localhost", 1)) {

      @Override
      public Set<Host> getHostsInState(HostState state) {
        switch (state) {
          case SERVING:
            return getHosts();
          case OFFLINE:
            return Collections.EMPTY_SET;
          default:
            throw new IllegalStateException(state.toString());
        }
      }
    };
    MRG rg = new MRG(1, 2, r1);
    getFunc().manageTransitions(rg);

    assertTrue("r1 should have been set to update complete", r1.updateCompleteCalled);
    assertEquals(RingState.UP, r1.getState());
  }
}
