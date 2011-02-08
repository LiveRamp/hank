package com.rapleaf.hank.config;

import java.util.Set;

import com.rapleaf.hank.coordinator.RingState;
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
  public Set<PartDaemonAddress> getHosts() {
    return hosts;
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
}
