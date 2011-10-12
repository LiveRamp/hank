package com.rapleaf.hank.partition_assigner;

import com.rapleaf.hank.coordinator.DomainGroupVersion;
import com.rapleaf.hank.coordinator.Ring;

import java.io.IOException;

public interface PartitionAssigner {
  public void assign(DomainGroupVersion domainGroupVersion, Ring ring) throws IOException;
}
