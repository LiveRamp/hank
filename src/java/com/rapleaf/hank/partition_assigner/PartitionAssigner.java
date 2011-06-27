package com.rapleaf.hank.partition_assigner;

import java.io.IOException;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.RingGroup;

public interface PartitionAssigner {
  public void assign(RingGroup ringGroup, int ringNum, Domain domain) throws IOException;
}
