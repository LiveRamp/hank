package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MockRingConfig implements RingConfig {

  private RingState state;
  private final int number;
  private final RingGroupConfig rgc;
  private final Set<HostConfig> hosts;
  public HostCommand allCommanded;

  public MockRingConfig(Set<PartDaemonAddress> hosts, RingGroupConfig rgc, int number, RingState state) {
    this.hosts = new HashSet<HostConfig>();
    if (hosts != null) {
      for (PartDaemonAddress addy : hosts) {
        this.hosts.add(new MockHostConfig(addy));
      }
    }
    this.rgc = rgc;
    this.number = number;
    this.state = state;
  }

  @Override
  public Set<HostConfig> getHosts() {
    return hosts;
  }

  @Override
  public RingGroupConfig getRingGroupConfig() {
    return rgc;
  }

  @Override
  public int getRingNumber() {
    return number;
  }

  @Override
  public RingState getState() {
    return state;
  }

  @Override
  public Integer getVersionNumber() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Integer getUpdatingToVersionNumber() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isUpdatePending() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void updateComplete() {
    // TODO Auto-generated method stub
    
  }

  public Integer getOldestVersionOnHosts() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public HostConfig addHost(PartDaemonAddress address) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HostConfig getHostConfigByAddress(PartDaemonAddress address) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<HostConfig> getHostsForDomainPartition(int domainId,
      int partition) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setState(RingState newState) throws IOException {
    state = newState;
  }

  @Override
  public int getNumHostsInState(HostState state) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void commandAll(HostCommand command) throws IOException {
    this.allCommanded = command;
  }
}
