package com.rapleaf.hank.config;

import com.rapleaf.hank.coordinator.Coordinator;

public class YamlSmartClientDaemonConfigurator implements
    SmartClientDaemonConfigurator {

  public YamlSmartClientDaemonConfigurator(String path) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public int getNumThreads() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getPortNumber() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getRingGroupName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Coordinator getCoordinator() {
    // TODO Auto-generated method stub
    return null;
  }

}
