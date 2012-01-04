package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MockHostDomain extends AbstractHostDomain {

  protected final Domain domain;
  private final Set<HostDomainPartition> parts = new HashSet<HostDomainPartition>();
  private boolean deleted = false;

  public MockHostDomain(Domain domain, int... numberAndVersionPairs) {
    this.domain = domain;

    for (int i = 0; i < numberAndVersionPairs.length; i += 2) {
      parts.add(new MockHostDomainPartition(numberAndVersionPairs[i],
          numberAndVersionPairs[i + 1]));
    }
  }

  @Override
  public HostDomainPartition addPartition(int partNum) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete() throws IOException {
    deleted = true;
  }

  @Override
  public Domain getDomain() {
    return domain;
  }

  @Override
  public Set<HostDomainPartition> getPartitions() throws IOException {
    return parts;
  }

  public boolean isDeleted() {
    return deleted;
  }
}
