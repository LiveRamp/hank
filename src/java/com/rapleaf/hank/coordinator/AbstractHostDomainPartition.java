package com.rapleaf.hank.coordinator;


import java.io.IOException;

public abstract class AbstractHostDomainPartition implements HostDomainPartition {

  @Override
  public int compareTo(HostDomainPartition o) {
    return Integer.valueOf(getPartitionNumber()).compareTo(o.getPartitionNumber());
  }

  @Override
  public String toString() {
    try {
      return "AbstractHostDomainPartition [partition number=" + getPartitionNumber() +
          ", current domain group version=" + getCurrentDomainGroupVersion() +
          ", deletable=" + isDeletable() + "]";
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
