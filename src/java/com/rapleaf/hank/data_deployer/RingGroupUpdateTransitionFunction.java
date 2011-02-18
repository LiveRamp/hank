package com.rapleaf.hank.data_deployer;

import com.rapleaf.hank.config.RingGroupConfig;

public interface RingGroupUpdateTransitionFunction {
  public void manageTransitions(RingGroupConfig ringGroup);
}
