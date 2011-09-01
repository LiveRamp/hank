package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractHost implements Host {
  @Override
  public int compareTo(Host o) {
    return getAddress().compareTo(o.getAddress());
  }

  @Override
  public Long getAggregateCount(String countID) throws IOException {
    long aggregateCount = new Long(0);
    boolean notNull = false;
    for (HostDomain hostDomain : getAssignedDomains()) {
      Long currentCount = hostDomain.getAggregateCount(countID);
      if (currentCount != null) {
        notNull = true;
        aggregateCount += currentCount;
      }
    }
    if (notNull) {
      return aggregateCount;
    }
    return null;
  }

  @Override
  public Set<String> getAggregateCountKeys() throws IOException {
    Set<String> aggregateCountKeys = new HashSet<String>();
    for (HostDomain hostDomain : getAssignedDomains()) {
      aggregateCountKeys.addAll(hostDomain.getAggregateCountKeys());
    }
    return aggregateCountKeys;
  }

  @Override
  public boolean isOnline() throws IOException {
    return getState() != HostState.OFFLINE;
  }

  @Override
  public HostDomain getHostDomain(Domain domain) {
    // TODO: this should be done with a map and caching
    try {
      for (HostDomain hdc : getAssignedDomains()) {
        if (hdc.getDomain() == domain) {
          return hdc;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  @Override
  public String toString() {
    return String.format("AbstractHost [address=%s]", this.getAddress());
  }
}
