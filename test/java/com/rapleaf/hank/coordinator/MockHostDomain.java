package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

public class MockHostDomain extends AbstractHostDomain {
  private final int domainId;
  private final Set<HostDomainPartition> parts = new HashSet<HostDomainPartition>();

  public MockHostDomain(int domainId, int... triples) {
    this.domainId = domainId;

    for (int i = 0; i < triples.length; i += 3) {
      parts.add(new MockHostDomainPartition(triples[i], triples[i+1], triples[i+2]));
    }
  }

  @Override
  public HostDomainPartition addPartition(int partNum, int initialVersion)  {
    throw new NotImplementedException();
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
