package com.rapleaf.hank.coordinator;

public abstract class AbstractDomainGroupVersionDomainVersion implements DomainGroupVersionDomainVersion {
  @Override
  public int compareTo(DomainGroupVersionDomainVersion o) {
    return getDomain().getName().compareTo(o.getDomain().getName());
  }

  @Override
  public String toString() {
    return getDomain().getName() + "@v" + getVersion();
  }
}
