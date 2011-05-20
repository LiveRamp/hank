package com.rapleaf.hank.coordinator.in_memory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.rapleaf.hank.coordinator.HostDomain;
import com.rapleaf.hank.coordinator.HostDomainPartition;

public class MemHostDomain implements HostDomain {
  private final int domainId;
  private final Set<HostDomainPartition> parts = new HashSet<HostDomainPartition>();

  public MemHostDomain(int domainId) {
    this.domainId = domainId;
  }

  @Override
  public HostDomainPartition addPartition(int partNum, int initialVersion) {
    MemHostDomainPartition hdpc = new MemHostDomainPartition(partNum, initialVersion);
    parts.add(hdpc);
    return hdpc;
  }

  @Override
  public int getDomainId() {
    return domainId;
  }

  @Override
  public Set<HostDomainPartition> getPartitions() throws IOException {
    return parts;
  }
}
