package com.rapleaf.hank.coordinator.in_memory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.rapleaf.hank.coordinator.HostDomainConfig;
import com.rapleaf.hank.coordinator.HostDomainPartitionConfig;

public class MemHostDomainConfig implements HostDomainConfig {
  private final int domainId;
  private final Set<HostDomainPartitionConfig> parts = new HashSet<HostDomainPartitionConfig>();

  public MemHostDomainConfig(int domainId) {
    this.domainId = domainId;
  }

  @Override
  public HostDomainPartitionConfig addPartition(int partNum, int initialVersion) {
    MemHostDomainPartitionConfig hdpc = new MemHostDomainPartitionConfig(partNum, initialVersion);
    parts.add(hdpc);
    return hdpc;
  }

  @Override
  public int getDomainId() {
    return domainId;
  }

  @Override
  public Set<HostDomainPartitionConfig> getPartitions() throws IOException {
    return parts;
  }
}
