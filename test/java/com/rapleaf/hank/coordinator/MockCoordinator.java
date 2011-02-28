package com.rapleaf.hank.coordinator;

import java.util.Map;
import java.util.Set;

import com.rapleaf.hank.exception.DataNotFoundException;

public class MockCoordinator implements Coordinator {

  private final Map<String, String> options;

  public static class Factory implements CoordinatorFactory {

    @Override
    public Coordinator getCoordinator(Map<String, String> options) {
      return new MockCoordinator(options);
    }
    
  }
  
  public MockCoordinator(Map<String, String> options) {
    this.options = options;
  }

  public MockCoordinator() {
    options = null;
  }

  @Override
  public DomainConfig getDomainConfig(String domainName)
      throws DataNotFoundException {
    return null;
  }

  @Override
  public DomainGroupConfig getDomainGroupConfig(String domainGroupName)
      throws DataNotFoundException {
    return null;
  }

  @Override
  public RingGroupConfig getRingGroupConfig(String ringGroupName)
      throws DataNotFoundException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int updateDomain(String domainName) throws DataNotFoundException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Set<DomainConfig> getDomainConfigs() {
    // TODO Auto-generated method stub
    return null;
  }

  public Set<DomainGroupConfig> getDomainGroupConfigs() {
    return null;
  }

  public Set<RingGroupConfig> getRingGroups() {
    return null;
  }

  public Map<String, String> getInitOptions() {
    return options;
  }

}
