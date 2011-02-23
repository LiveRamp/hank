package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.Set;

public interface HostDomainConfig {
  public byte getDomainId();

  public Set<HostDomainPartitionConfig> getPartitions() throws IOException;

  public void addPartition(int partNum, int initialVersion) throws Exception;
}
