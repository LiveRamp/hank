package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.Set;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class MockRingConfig implements RingConfig {

  private final RingState state;
  private final int number;
  private final RingGroupConfig rgc;
  private final Set<PartDaemonAddress> hosts;

  public MockRingConfig(Set<PartDaemonAddress> hosts, RingGroupConfig rgc,
      int number, RingState state) {
    this.hosts = hosts;
    this.rgc = rgc;
    this.number = number;
    this.state = state;
  }

  @Override
  public Set<HostConfig> getHosts() {
    throw new NotImplementedException();
//    return hosts;
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
  public void startAllPartDaemons() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void startAllUpdaters() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void takeDownPartDaemons() {
    // TODO Auto-generated method stub
    
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
      int partition) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setState(RingState newState) throws IOException {
    // TODO Auto-generated method stub
    
  }
}
