package com.rapleaf.hank.config;

import com.rapleaf.hank.coordinator.Coordinator;

public interface DataDeployerConfigurator {

  public String getRingGroupName();

  public Coordinator getCoordinator();

  public long getSleepInterval();

}
