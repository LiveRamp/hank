package com.rapleaf.hank.coordinator;

import java.io.IOException;


public abstract class AbstractHostDomain implements HostDomain {
  @Override
  public int compareTo(HostDomain arg0) {
    return Integer.valueOf(getDomainId()).compareTo(arg0.getDomainId());
  }

  @Override
  public HostDomainPartition getPartitionByNumber(int partNum) throws IOException {
    for (HostDomainPartition p : getPartitions()) {
      if (p.getPartNum() == partNum) {
        return p;
      }
    }
    return null;
  }
}
