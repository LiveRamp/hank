package com.rapleaf.hank.coordinator;

public abstract class AbstractDomainGroupVersionDomainVersion implements DomainGroupVersionDomainVersion {
  @Override
  public int compareTo(DomainGroupVersionDomainVersion o) {
    return getDomain().getName().compareTo(o.getDomain().getName());
  }
}
