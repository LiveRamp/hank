package com.rapleaf.hank.coordinator;

import java.util.Set;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.rapleaf.hank.exception.DataNotFoundException;

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
  public Set<Integer> getDomainPartitionsForHost(PartDaemonAddress hostAndPort,
      int domainId) throws DataNotFoundException {
    return null;
  }

  @Override
  public Set<HostConfig> getHosts() {
    throw new NotImplementedException();
//    return hosts;
  }

  @Override
  public Set<PartDaemonAddress> getHostsForDomainPartition(int domainId,
      int partId) {
    return null;
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
  public int getVersionNumber() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getUpdatingToVersionNumber() {
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

  public int getOldestVersionOnHosts() {
    // TODO Auto-generated method stub
    return 0;
  }
}
