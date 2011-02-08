package com.rapleaf.hank.config;

import java.util.Set;

import com.rapleaf.hank.exception.DataNotFoundException;

public class MockRingGroupConfig implements RingGroupConfig {

  private final DomainGroupConfig dcg;
  private final String name;
  private final Set<RingConfig> ringConfigs;

  public MockRingGroupConfig(DomainGroupConfig dcg, String name, Set<RingConfig> ringConfigs) {
    this.dcg = dcg;
    this.name = name;
    this.ringConfigs = ringConfigs;
    
  }

  @Override
  public DomainGroupConfig getDomainGroupConfig() {
    return dcg;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public RingConfig getRingConfig(int ringNumber) throws DataNotFoundException {
    return null;
  }

  @Override
  public RingConfig getRingConfigForHost(PartDaemonAddress hostAddress)
  throws DataNotFoundException {
    return null;
  }

  @Override
  public Set<RingConfig> getRingConfigs() {
    return ringConfigs;
  }
}
