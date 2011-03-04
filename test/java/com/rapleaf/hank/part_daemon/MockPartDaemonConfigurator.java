package com.rapleaf.hank.part_daemon;

import java.util.Collections;
import java.util.Set;

import com.rapleaf.hank.config.PartservConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;

public class MockPartDaemonConfigurator implements PartservConfigurator {

  private final int servicePort;
  private final Coordinator coordinator;
  private final String ringGroupName;
  private final String localDataDir;

  public MockPartDaemonConfigurator(int servicePort, Coordinator coordinator, String ringGroupName, String localDataDir) {
    this.servicePort = servicePort;
    this.coordinator = coordinator;
    this.ringGroupName = ringGroupName;
    this.localDataDir = localDataDir;
  }

  @Override
  public Set<String> getLocalDataDirectories() {
    return Collections.singleton(localDataDir);
  }

  @Override
  public int getServicePort() {
    return servicePort;
  }

  @Override
  public Coordinator getCoordinator() {
    return coordinator;
  }

  @Override
  public String getRingGroupName() {
    return ringGroupName;
  }

  @Override
  public int getNumThreads() {
    return 1;
  }

  @Override
  public int getNumConcurrentUpdates() {
    return 1;
  }
}
