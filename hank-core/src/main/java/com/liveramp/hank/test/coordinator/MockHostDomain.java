package com.liveramp.hank.test.coordinator;

import com.liveramp.hank.coordinator.AbstractHostDomain;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.HostDomainPartition;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class MockHostDomain extends AbstractHostDomain {

  protected final Domain domain;
  private final Set<HostDomainPartition> parts = new HashSet<HostDomainPartition>();
  private Set<Integer> removedPartitions = new TreeSet<Integer>();

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
  public void removePartition(int partNum) throws IOException {
    removedPartitions.add(partNum);
  }

  public boolean isRemoved(int partitionNumber) {
    return removedPartitions.contains(partitionNumber);
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
