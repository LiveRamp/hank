package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MockHostDomain extends AbstractHostDomain {

  protected final Domain domain;
  private final Set<HostDomainPartition> parts = new HashSet<HostDomainPartition>();
  private boolean deleted = false;
  private Map<String, String> statistics = new HashMap<String, String>();

  public MockHostDomain(Domain domain, int... numberAndVersionAndUpdatingToVersionTriples) {
    this.domain = domain;

    for (int i = 0; i < numberAndVersionAndUpdatingToVersionTriples.length; i += 3) {
      parts.add(new MockHostDomainPartition(numberAndVersionAndUpdatingToVersionTriples[i],
          numberAndVersionAndUpdatingToVersionTriples[i + 1],
          numberAndVersionAndUpdatingToVersionTriples[i + 2]));
    }
  }

  @Override
  public HostDomainPartition addPartition(int partNum, int initialVersion) {
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

  @Override
  public void setEphemeralStatistic(String key, String value) throws IOException {
    statistics.put(key, value);
  }

  @Override
  public String getStatistic(String key) throws IOException {
    return statistics.get(key);
  }

  @Override
  public void deleteStatistic(String key) throws IOException {
    statistics.remove(key);
  }
}
