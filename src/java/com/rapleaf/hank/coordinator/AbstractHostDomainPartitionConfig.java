package com.rapleaf.hank.coordinator;


public abstract class AbstractHostDomainPartitionConfig implements HostDomainPartitionConfig {
  @Override
  public int compareTo(HostDomainPartitionConfig o) {
    return Integer.valueOf(getPartNum()).compareTo(o.getPartNum());
  }
}
