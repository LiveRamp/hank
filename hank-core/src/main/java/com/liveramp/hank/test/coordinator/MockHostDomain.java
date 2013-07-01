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
  private final Set<HostDomainPartition> partitions = new HashSet<HostDomainPartition>();
  private Set<Integer> removedPartitions = new TreeSet<Integer>();

  public MockHostDomain(Domain domain, int... partitionNumberAndVersionPairs) {
    this.domain = domain;

    for (int i = 0; i < partitionNumberAndVersionPairs.length; i += 2) {
      partitions.add(new MockHostDomainPartition(partitionNumberAndVersionPairs[i],
          partitionNumberAndVersionPairs[i + 1]));
    }
  }

  @Override
  public HostDomainPartition addPartition(int partitionNumber) {
    MockHostDomainPartition result = new MockHostDomainPartition(partitionNumber, null);
    partitions.add(result);
    return result;
  }

  @Override
  public void removePartition(int partitionNumber) throws IOException {
    removedPartitions.add(partitionNumber);
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
    return partitions;
  }
}
