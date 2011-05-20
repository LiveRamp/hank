package com.rapleaf.hank.coordinator.in_memory;

import java.util.Set;

import com.rapleaf.hank.coordinator.DomainGroupVersionDomainVersion;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.DomainGroupVersion;

public class MemDomainGroupVersion implements DomainGroupVersion, Comparable<MemDomainGroupVersion> {

  private final Set<DomainGroupVersionDomainVersion> domainIdToVersion;
  private final DomainGroup parent;
  private final int v;

  public MemDomainGroupVersion(Set<DomainGroupVersionDomainVersion> x, DomainGroup parent, int v) {
    this.domainIdToVersion = x;
    this.parent = parent;
    this.v = v;
  }

  @Override
  public Set<DomainGroupVersionDomainVersion> getDomainVersions() {
    return domainIdToVersion;
  }

  @Override
  public DomainGroup getDomainGroup() {
    return parent;
  }

  @Override
  public int getVersionNumber() {
    return v;
  }

  @Override
  public int compareTo(MemDomainGroupVersion arg0) {
    return Integer.valueOf(v).compareTo(arg0.v);
  }
}
