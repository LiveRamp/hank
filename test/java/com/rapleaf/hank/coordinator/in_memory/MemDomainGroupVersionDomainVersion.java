package com.rapleaf.hank.coordinator.in_memory;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroupVersionDomainVersion;

public class MemDomainGroupVersionDomainVersion implements DomainGroupVersionDomainVersion {
  private final Domain domainConfig;
  private final Integer ver;

  public MemDomainGroupVersionDomainVersion(Domain byName, Integer value) {
    this.domainConfig = byName;
    this.ver = value;
  }

  @Override
  public Domain getDomainConfig() {
    return domainConfig;
  }

  @Override
  public int getVersionNumber() {
    return ver;
  }
}
