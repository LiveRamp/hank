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

public class InMemoryCoordinator implements Coordinator {

  private final Map<String, MemDomainConfig> domains = new HashMap<String, MemDomainConfig>();
  private final Map<String, MemDomainGroupConfig> domain_groups = new HashMap<String, MemDomainGroupConfig>();
  private final Map<String, MemRingGroupConfig> ring_groups = new HashMap<String, MemRingGroupConfig>();


  @Override
  public DomainConfig addDomain(String domainName, int numParts, String storageEngineFactoryName, String storageEngineOptions, String partitionerName) throws IOException {
    MemDomainConfig domainConfig = new MemDomainConfig(domainName, numParts, storageEngineFactoryName, storageEngineOptions, partitionerName);
    domains.put(domainName, domainConfig);
    return domainConfig;
  }

  @Override
  public DomainGroupConfig addDomainGroup(String name) throws IOException {
    MemDomainGroupConfig dgc = new MemDomainGroupConfig(name);
    domain_groups.put(name, dgc);
    return dgc;
  }

  @Override
  public RingGroupConfig addRingGroup(String ringGroupName, String domainGroupName) throws IOException {
    MemRingGroupConfig rgc = new MemRingGroupConfig(ringGroupName, domain_groups.get(domainGroupName));
    ring_groups.put(ringGroupName, rgc);
    return rgc;
  }

  @Override
  public DomainConfig getDomainConfig(String domainName) {
    return domains.get(domainName);
  }

  @Override
  public Set<DomainConfig> getDomainConfigs() {
    return new HashSet<DomainConfig>(domains.values());
  }

  @Override
  public DomainGroupConfig getDomainGroupConfig(String domainGroupName) {
    return domain_groups.get(domainGroupName);
  }

  @Override
  public Set<DomainGroupConfig> getDomainGroupConfigs() {
    return new HashSet<DomainGroupConfig>(domain_groups.values());
  }

  @Override
  public RingGroupConfig getRingGroupConfig(String ringGroupName) {
    return ring_groups.get(ringGroupName);
  }

  @Override
  public Set<RingGroupConfig> getRingGroups() {
    return new HashSet<RingGroupConfig>(ring_groups.values());
  }

  @Override
  public boolean deleteDomainConfig(String domainName) {
    return domains.remove(domainName) != null;
  }
}
