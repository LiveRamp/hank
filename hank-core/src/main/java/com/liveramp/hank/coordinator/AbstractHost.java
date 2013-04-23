package com.liveramp.hank.coordinator;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

public abstract class AbstractHost implements Host {

  @Override
  public int compareTo(Host o) {
    return getAddress().compareTo(o.getAddress());
  }

  @Override
  public SortedSet<HostDomain> getAssignedDomainsSorted() throws IOException {
    return new TreeSet<HostDomain>(getAssignedDomains());
  }

  @Override
  public HostDomain getHostDomain(Domain domain) {
    // TODO: this should be done with a map and caching
    try {
      for (HostDomain hostDomain : getAssignedDomains()) {
        if (hostDomain.getDomain().equals(domain)) {
          return hostDomain;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  @Override
  public String toString() {
    return String.format("AbstractHost [address=%s]", getAddress());
  }
}
