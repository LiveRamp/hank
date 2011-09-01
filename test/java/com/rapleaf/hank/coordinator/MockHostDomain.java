package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MockHostDomain extends AbstractHostDomain {
  private final Domain domain;
  private final Set<HostDomainPartition> parts = new HashSet<HostDomainPartition>();

  public MockHostDomain(Domain domain, int... triples) {
    this.domain = domain;

    for (int i = 0; i < triples.length; i += 3) {
      parts.add(new MockHostDomainPartition(triples[i], triples[i + 1],
          triples[i + 2]));
    }
  }

  @Override
  public HostDomainPartition addPartition(int partNum, int initialVersion) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Domain getDomain() {
    return domain;
  }

  @Override
  public Set<HostDomainPartition> getPartitions() throws IOException {
    return parts;
  }
}
