package com.rapleaf.hank.coordinator;


public abstract class AbstractDomainGroupVersion implements DomainGroupVersion {
  @Override
  public int compareTo(DomainGroupVersion o) {
    return Integer.valueOf(getVersionNumber()).compareTo(o.getVersionNumber());
  }
}
