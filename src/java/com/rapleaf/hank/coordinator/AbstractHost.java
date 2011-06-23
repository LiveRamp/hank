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
    Long aggregateCount = new Long(0);
    Long currentCount = new Long(0);
    Boolean notNull = false;
    for (HostDomain hostDomain : getAssignedDomains()) {
      currentCount = hostDomain.getAggregateCount(countID);
      if (currentCount != null) {
        notNull = true;
        aggregateCount += currentCount;
      }
    }
    if (notNull) {
      return aggregateCount;
    } else {
      return null;
    }
  }

  @Override
  public Set<String> getAggregateCountKeys() throws IOException {
    Set<String> aggregateCountKeys = new HashSet<String>();
    for (HostDomain hostDomain : getAssignedDomains()) {
      aggregateCountKeys.addAll(hostDomain.getAggregateCountKeys());
    }
    return aggregateCountKeys;
  }
}
