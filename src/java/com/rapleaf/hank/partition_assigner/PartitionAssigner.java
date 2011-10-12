package com.rapleaf.hank.partition_assigner;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.RingGroup;

import java.io.IOException;

public interface PartitionAssigner {
  public void assign(RingGroup ringGroup, int ringNum, Domain domain) throws IOException;
}
