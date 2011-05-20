package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

public class MockHostDomain implements HostDomain {
  private final int domainId;
  private final Set<HostDomainPartitionConfig> parts = new HashSet<HostDomainPartitionConfig>();

  public MockHostDomain(int domainId, int... triples) {
    this.domainId = domainId;

    for (int i = 0; i < triples.length; i += 3) {
      parts.add(new MockHostDomainPartitionConfig(triples[i], triples[i+1], triples[i+2]));
    }
  }

  @Override
  public HostDomainPartitionConfig addPartition(int partNum, int initialVersion)  {
    throw new NotImplementedException();
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
