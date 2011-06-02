package com.rapleaf.hank.coordinator;


public abstract class AbstractHostDomain implements HostDomain {
  @Override
  public int compareTo(HostDomain arg0) {
    return Integer.valueOf(getDomainId()).compareTo(arg0.getDomainId());
  }
}
