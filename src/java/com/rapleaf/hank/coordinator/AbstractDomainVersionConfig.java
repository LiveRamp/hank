package com.rapleaf.hank.coordinator;

public abstract class AbstractDomainVersionConfig implements DomainVersionConfig {
  @Override
  public int compareTo(DomainVersionConfig arg0) {
    return Integer.valueOf(getVersionNumber()).compareTo(arg0.getVersionNumber());
  }
}
