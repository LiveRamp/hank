package com.rapleaf.hank.config;


public interface DataDeployerConfigurator extends Configurator {
  public String getRingGroupName();

  public long getSleepInterval();
}
