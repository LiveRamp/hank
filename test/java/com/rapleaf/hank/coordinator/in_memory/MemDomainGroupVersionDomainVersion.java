package com.rapleaf.hank.coordinator.in_memory;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroupVersionDomainVersion;

public class MemDomainGroupVersionDomainVersion implements DomainGroupVersionDomainVersion {
  private final Domain domain;
  private final Integer ver;

  public MemDomainGroupVersionDomainVersion(Domain byName, Integer value) {
    this.domain = byName;
    this.ver = value;
  }

  public Domain getDomain() {
    return domain;
  }

  public int getVersionNumber() {
    return ver;
  }
}
