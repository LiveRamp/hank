package com.rapleaf.hank.coordinator;


public abstract class AbstractHostDomainPartition implements HostDomainPartition {
  @Override
  public int compareTo(HostDomainPartition o) {
    return Integer.valueOf(getPartNum()).compareTo(o.getPartNum());
  }
}
