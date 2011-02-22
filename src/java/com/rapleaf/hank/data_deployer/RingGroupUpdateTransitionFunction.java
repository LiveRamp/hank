package com.rapleaf.hank.data_deployer;

import java.io.IOException;

import com.rapleaf.hank.coordinator.RingGroupConfig;

public interface RingGroupUpdateTransitionFunction {
  public void manageTransitions(RingGroupConfig ringGroup) throws IOException;
}
