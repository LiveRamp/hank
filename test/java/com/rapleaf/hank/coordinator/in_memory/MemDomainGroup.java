package com.rapleaf.hank.coordinator.in_memory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang.NotImplementedException;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.DomainGroupChangeListener;
import com.rapleaf.hank.coordinator.DomainGroupVersion;
import com.rapleaf.hank.coordinator.DomainGroupVersionDomainVersion;

public class MemDomainGroup implements DomainGroup {
  private final String name;
  private final Map<Integer, Domain> domains = new HashMap<Integer, Domain>();
  private final SortedMap<Integer, DomainGroupVersion> versions = new TreeMap<Integer, DomainGroupVersion>();

  public MemDomainGroup(String name) {
    this.name = name;
  }

  public void addDomain(Domain domain, int domainId) throws IOException {
    this.domains.put(domainId, domain);
  }

  public DomainGroupVersion createNewVersion(Map<String, Integer> domainIdToVersion) throws IOException {
    Set<DomainGroupVersionDomainVersion> x = new HashSet<DomainGroupVersionDomainVersion>();
    for (Map.Entry<String, Integer> e : domainIdToVersion.entrySet()) {
      x.add(new MemDomainGroupVersionDomainVersion(getByName(e.getKey()), e.getValue()));
    }
    int verNum = (versions.isEmpty() ? 0 : versions.lastKey()) + 1;
    DomainGroupVersion v = new MemDomainGroupVersion(x, this, verNum);
    versions.put(verNum, v);
    return v;
  }

  private Domain getByName(String key) {
    for (Map.Entry<Integer, Domain> dc : domains.entrySet()) {
      if (dc.getValue().getName().equals(key)) {
        return dc.getValue();
      }
    }
    throw new IllegalStateException();
  }

  public Domain getDomain(int domainId) {
    return domains.get(domainId);
  }

  public Integer getDomainId(String domainName) {
    for (Map.Entry<Integer, Domain> dc : domains.entrySet()) {
      if (dc.getValue().getName().equals(domainName)) {
        return dc.getKey();
      }
    }
    return null;
  }

  public DomainGroupVersion getLatestVersion() throws IOException {
    if (versions.isEmpty()) {
      return null;
    }
    return versions.get(versions.lastKey());
  }

  public String getName() {
    return name;
  }

  public SortedSet<DomainGroupVersion> getVersions() throws IOException {
    return new TreeSet<DomainGroupVersion>(versions.values());
  }

  public void setListener(DomainGroupChangeListener listener) {
    throw new NotImplementedException();
  }

  public Set<Domain> getDomains() throws IOException {
    return new HashSet<Domain>(domains.values());
  }
}
