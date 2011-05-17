package com.rapleaf.hank.coordinator.in_memory;

import java.util.Set;

import com.rapleaf.hank.coordinator.DomainGroupVersionDomainVersion;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfigVersion;

public class MemDomainGroupConfigVersion implements DomainGroupConfigVersion, Comparable<MemDomainGroupConfigVersion> {

  private final Set<DomainGroupVersionDomainVersion> domainIdToVersion;
  private final DomainGroupConfig parent;
  private final int v;

  public MemDomainGroupConfigVersion(Set<DomainGroupVersionDomainVersion> x, DomainGroupConfig parent, int v) {
    this.domainIdToVersion = x;
    this.parent = parent;
    this.v = v;
  }

  @Override
  public Set<DomainGroupVersionDomainVersion> getDomainConfigVersions() {
    return domainIdToVersion;
  }

  @Override
  public DomainGroupConfig getDomainGroupConfig() {
    return parent;
  }

  @Override
  public int getVersionNumber() {
    return v;
  }

  @Override
  public int compareTo(MemDomainGroupConfigVersion arg0) {
    return Integer.valueOf(v).compareTo(arg0.v);
  }
}
