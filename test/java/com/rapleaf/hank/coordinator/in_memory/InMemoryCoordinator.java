package com.rapleaf.hank.coordinator.in_memory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.RingGroupConfig;

public class InMemoryCoordinator implements Coordinator {

  private final Map<String, MemDomain> domains = new HashMap<String, MemDomain>();
  private final Map<String, MemDomainGroup> domain_groups = new HashMap<String, MemDomainGroup>();
  private final Map<String, MemRingGroupConfig> ring_groups = new HashMap<String, MemRingGroupConfig>();


  @Override
  public Domain addDomain(String domainName, int numParts, String storageEngineFactoryName, String storageEngineOptions, String partitionerName) throws IOException {
    MemDomain domainConfig = new MemDomain(domainName, numParts, storageEngineFactoryName, storageEngineOptions, partitionerName);
    domains.put(domainName, domainConfig);
    return domainConfig;
  }

  @Override
  public DomainGroup addDomainGroup(String name) throws IOException {
    MemDomainGroup dgc = new MemDomainGroup(name);
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
  public Domain getDomainConfig(String domainName) {
    return domains.get(domainName);
  }

  @Override
  public Set<Domain> getDomainConfigs() {
    return new HashSet<Domain>(domains.values());
  }

  @Override
  public DomainGroup getDomainGroupConfig(String domainGroupName) {
    return domain_groups.get(domainGroupName);
  }

  @Override
  public Set<DomainGroup> getDomainGroupConfigs() {
    return new HashSet<DomainGroup>(domain_groups.values());
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
