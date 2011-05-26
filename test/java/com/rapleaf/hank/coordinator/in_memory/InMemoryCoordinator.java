package com.rapleaf.hank.coordinator.in_memory;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.RingGroup;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class InMemoryCoordinator implements Coordinator {

  private final Map<String, MemDomain> domains = new HashMap<String, MemDomain>();
  private final Map<String, MemDomainGroup> domainGroups = new HashMap<String, MemDomainGroup>();
  private final Map<String, MemRingGroup> ringGroups = new HashMap<String, MemRingGroup>();


  public Domain addDomain(String domainName, int numParts, String storageEngineFactoryName, String storageEngineOptions, String partitionerName) throws IOException {
    MemDomain domain = new MemDomain(domainName, numParts, storageEngineFactoryName, storageEngineOptions, partitionerName);
    domains.put(domainName, domain);
    return domain;
  }

  public DomainGroup addDomainGroup(String name) throws IOException {
    MemDomainGroup dgc = new MemDomainGroup(name);
    domainGroups.put(name, dgc);
    return dgc;
  }

  public RingGroup addRingGroup(String ringGroupName, String domainGroupName) throws IOException {
    MemRingGroup rgc = new MemRingGroup(ringGroupName, domainGroups.get(domainGroupName));
    ringGroups.put(ringGroupName, rgc);
    return rgc;
  }

  public Domain getDomain(String domainName) {
    return domains.get(domainName);
  }

  public Set<Domain> getDomains() {
    return new HashSet<Domain>(domains.values());
  }

  public DomainGroup getDomainGroupConfig(String domainGroupName) {
    return domainGroups.get(domainGroupName);
  }

  public Set<DomainGroup> getDomainGroups() {
    return new HashSet<DomainGroup>(domainGroups.values());
  }

  public RingGroup getRingGroupConfig(String ringGroupName) {
    return ringGroups.get(ringGroupName);
  }

  public Set<RingGroup> getRingGroups() {
    return new HashSet<RingGroup>(ringGroups.values());
  }

  public boolean deleteDomain(String domainName) {
    return domains.remove(domainName) != null;
  }
}
