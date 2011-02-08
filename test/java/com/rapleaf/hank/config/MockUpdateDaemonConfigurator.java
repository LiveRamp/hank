package com.rapleaf.hank.config;

import java.util.Set;

import com.rapleaf.hank.coordinator.Coordinator;

public class MockUpdateDaemonConfigurator implements UpdateDaemonConfigurator {
  private final int numConcurrentUpdates;
  private final Set<String> localDataDirs;
  private final int servicePort;
  private final Coordinator coord;
  private final String ringGroupName;
  private final int ringNum;

  public MockUpdateDaemonConfigurator(int numConcurrentUpdates, Set<String> localDataDirs, int servicePort, Coordinator coord, String ringGroupName, int ringNum) {
    this.numConcurrentUpdates = numConcurrentUpdates;
    this.localDataDirs = localDataDirs;
    this.servicePort = servicePort;
    this.coord = coord;
    this.ringGroupName = ringGroupName;
    this.ringNum = ringNum;
  }

  @Override
  public int getNumConcurrentUpdates() {
    return numConcurrentUpdates;
  }

  @Override
  public Set<String> getLocalDataDirectories() {
    return localDataDirs;
  }

  @Override
  public int getServicePort() {
    return servicePort;
  }

  @Override
  public Coordinator getCoordinator() {
    return coord;
  }

  @Override
  public String getRingGroupName() {
    return ringGroupName;
  }

  @Override
  public int getRingNumber() {
    return ringNum;
  }
}
