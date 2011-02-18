package com.rapleaf.hank.data_deployer;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashSet;

import junit.framework.TestCase;

import com.rapleaf.hank.config.MockRingConfig;
import com.rapleaf.hank.config.MockRingGroupConfig;
import com.rapleaf.hank.config.PartDaemonAddress;
import com.rapleaf.hank.config.RingConfig;
import com.rapleaf.hank.coordinator.RingState;

public class TestDaemon extends TestCase {

  private class MRC extends MockRingConfig {
    private boolean takeDownPartDaemonsCalled;
    private final boolean updatePending;
    private final int curVer;
    private final int nextVer;
    public boolean startAllUpdatersCalled;
    private boolean startAllPartDaemonsCalled;
    public boolean updateCompleteCalled;

    public MRC(int number, RingState state, boolean updatePending, int curVer, int nextVer, PartDaemonAddress... hosts) {
      super(new LinkedHashSet<PartDaemonAddress>(Arrays.asList(hosts)), null, number, state);
      this.updatePending = updatePending;
      this.curVer = curVer;
      this.nextVer = nextVer;
    }

    @Override
    public void takeDownPartDaemons() {
      this.takeDownPartDaemonsCalled = true;
    }

    @Override
    public boolean isUpdatePending() {
      return updatePending;
    }

    @Override
    public int getUpdatingToVersionNumber() {
      return nextVer;
    }

    @Override
    public int getVersionNumber() {
      return curVer;
    }

    @Override
    public void startAllPartDaemons() {
      this.startAllPartDaemonsCalled = true;
    }

    @Override
    public void startAllUpdaters() {
      this.startAllUpdatersCalled = true;
    }

    @Override
    public void updateComplete() {
      this.updateCompleteCalled = true;
    }
  }


  private class MRG extends MockRingGroupConfig {
    private final int curVer;
    private final int toVer;

    public MRG(int curVer, int toVer, RingConfig... ringConfigs) {
      super(null, "myRingGroup", new LinkedHashSet<RingConfig>(Arrays.asList(ringConfigs)));
      this.curVer = curVer;
      this.toVer = toVer;
    }

    @Override
    public int getCurrentVersion() {
      return curVer;
    }

    @Override
    public boolean isUpdating() {
      return true;
    }
  }

  public void testDownsFirstAvailableRing() throws Exception {
    MRC r1 = new MRC(1, RingState.AVAILABLE, true, 1, 2);
    MRC r2 = new MRC(2, RingState.AVAILABLE, true, 1, 2);
    MRG rg = new MRG(1, 2, r1, r2);
    Daemon.manageTransitions(rg, null);

    assertTrue("r1 should have been taken down", r1.takeDownPartDaemonsCalled);
    assertFalse("r2 should not have been taken down", r2.takeDownPartDaemonsCalled);
  }

  public void testDownsOnlyAvailableRing() throws Exception {
    MRC r1 = new MRC(1, RingState.AVAILABLE, false, 2, 2);
    MRC r2 = new MRC(2, RingState.AVAILABLE, true, 1, 2);
    MRG rg = new MRG(1, 2, r1, r2);
    Daemon.manageTransitions(rg, null);

    assertFalse("r1 should not have been taken down", r1.takeDownPartDaemonsCalled);
    assertTrue("r2 should have been taken down", r2.takeDownPartDaemonsCalled);
  }

  public void testDownsNothingIfSomethingIsAlreadyDown() throws Exception {
    for(RingState s : EnumSet.of(RingState.STOPPING, RingState.STARTING, RingState.UPDATING, RingState.IDLE)) {
      MRC r1 = new MRC(1, s, true, 1, 2);
      MRC r2 = new MRC(2, RingState.AVAILABLE, true, 1, 2);
      MRG rg = new MRG(1, 2, r1, r2);
      Daemon.manageTransitions(rg, null);

      assertFalse("r1 should not have been taken down", r1.takeDownPartDaemonsCalled);
      assertFalse("r2 should not have been taken down", r2.takeDownPartDaemonsCalled); 
    }
  }

  public void testIdleToUpdating() throws Exception {
    MRC r1 = new MRC(1, RingState.IDLE, true, 1, 2);
    MRG rg = new MRG(1, 2, r1);
    Daemon.manageTransitions(rg, null);

    assertTrue("r1 should have been set to updating", r1.startAllUpdatersCalled);
  }

  public void testIdleToStarting() throws Exception {
    MRC r1 = new MRC(1, RingState.IDLE, true, 1, 2) {
      @Override
      public int getOldestVersionOnHosts() {
        return 2;
      }
    };
    MRG rg = new MRG(1, 2, r1);
    Daemon.manageTransitions(rg, null);

    assertTrue("r1 should have been set to starting", r1.startAllPartDaemonsCalled);
  }

  public void testAvailableToUpdateComplete() throws Exception {
    MRC r1 = new MRC(1, RingState.AVAILABLE, true, 1, 2) {
      @Override
      public int getOldestVersionOnHosts() {
        return 2;
      }
    };
    MRG rg = new MRG(1, 2, r1);
    Daemon.manageTransitions(rg, null);

    assertTrue("r1 should have been set to update complete", r1.updateCompleteCalled);
  }
}
