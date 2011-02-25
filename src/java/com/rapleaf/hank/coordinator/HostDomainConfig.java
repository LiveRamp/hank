package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.Set;

public interface HostDomainConfig {
  public int getDomainId();

  public Set<HostDomainPartitionConfig> getPartitions() throws IOException;

  public HostDomainPartitionConfig addPartition(int partNum, int initialVersion) throws Exception;
}
