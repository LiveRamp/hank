package com.rapleaf.hank.coordinator.in_memory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.exception.DataNotFoundException;

public class InMemoryCoordinator implements Coordinator {

  private final Map<String, MemDomainConfig> domains = new HashMap<String, MemDomainConfig>();
  private final Map<String, MemDomainGroupConfig> domain_groups = new HashMap<String, MemDomainGroupConfig>();
  private final Map<String, MemRingGroupConfig> ring_groups = new HashMap<String, MemRingGroupConfig>();


  @Override
  public void addDomain(String domainName, int numParts, String storageEngineFactoryName, String storageEngineOptions, String partitionerName, int initialVersion) throws IOException {
    domains.put(domainName, new MemDomainConfig(domainName, numParts, storageEngineFactoryName, storageEngineOptions, partitionerName, initialVersion));
  }

  @Override
  public DomainGroupConfig addDomainGroup(String name) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RingGroupConfig addRingGroup(String ringGroupName, String domainGroupName) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DomainConfig getDomainConfig(String domainName) throws DataNotFoundException {
    return domains.get(domainName);
  }

  @Override
  public Set<DomainConfig> getDomainConfigs() {
    return new HashSet<DomainConfig>(domains.values());
  }

  @Override
  public DomainGroupConfig getDomainGroupConfig(String domainGroupName) throws DataNotFoundException {
    return domain_groups.get(domainGroupName);
  }

  @Override
  public Set<DomainGroupConfig> getDomainGroupConfigs() {
    return new HashSet<DomainGroupConfig>(domain_groups.values());
  }

  @Override
  public RingGroupConfig getRingGroupConfig(String ringGroupName) throws DataNotFoundException {
    return ring_groups.get(ringGroupName);
  }

  @Override
  public Set<RingGroupConfig> getRingGroups() {
    return new HashSet<RingGroupConfig>(ring_groups.values());
  }
}
