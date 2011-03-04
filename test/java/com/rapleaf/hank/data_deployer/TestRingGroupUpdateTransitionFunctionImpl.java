package com.rapleaf.hank.data_deployer;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashSet;

import junit.framework.TestCase;

import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.MockRingConfig;
import com.rapleaf.hank.coordinator.MockRingGroupConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingState;

public class TestRingGroupUpdateTransitionFunctionImpl extends TestCase {
  private class MRC extends MockRingConfig {
    private final boolean updatePending;
    private final int curVer;
    private final int nextVer;

    public boolean updateCompleteCalled;

    public MRC(int number, RingState state, boolean updatePending, int curVer, int nextVer, PartDaemonAddress... hosts) {
      super(new LinkedHashSet<PartDaemonAddress>(Arrays.asList(hosts)), null, number, state);
      this.updatePending = updatePending;
      this.curVer = curVer;
      this.nextVer = nextVer;
    }

    @Override
    public boolean isUpdatePending() {
      return updatePending;
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
  }


  private class MRG extends MockRingGroupConfig {
    private final int curVer;
    @SuppressWarnings("unused")
    private final int toVer;

    public MRG(int curVer, int toVer, RingConfig... ringConfigs) {
      super(null, "myRingGroup", new LinkedHashSet<RingConfig>(Arrays.asList(ringConfigs)));
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
    for(RingState s : EnumSet.of(RingState.GOING_DOWN, RingState.COMING_UP, RingState.UPDATING, RingState.DOWN)) {
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
      public int getNumHostsInState(HostState state) {
        switch (state) {
          case OFFLINE:
            return 0;
          case UPDATING:
            return 0;
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
      public int getNumHostsInState(HostState state) {
        switch (state) {
          case OFFLINE:
            return 0;
          case UPDATING:
            return 1;
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
      public int getNumHostsInState(HostState state) {
        switch (state) {
          case OFFLINE:
            return 0;
          case UPDATING:
            return 0;
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
      public int getNumHostsInState(HostState state) {
        switch(state) {
          case SERVING:
            return 1;
          case OFFLINE:
            return 0;
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
