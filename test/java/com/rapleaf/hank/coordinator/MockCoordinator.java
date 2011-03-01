package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.rapleaf.hank.exception.DataNotFoundException;

public class MockCoordinator implements Coordinator {

  private final Map<String, Object> options;

  public static class Factory implements CoordinatorFactory {
    @Override
    public Coordinator getCoordinator(Map<String, Object> options) {
      return new MockCoordinator(options);
    }
  }

  public MockCoordinator(Map<String, Object> options) {
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

  public Map<String, Object> getInitOptions() {
    return options;
  }

  @Override
  public void addDomain(String domainName, int numParts,
      String storageEngineFactoryName, String storageEngineOptions,
      String partitionerName, int initialVersion) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void addDomainGroup(String name) throws IOException {
    // TODO Auto-generated method stub
    
  }

}
