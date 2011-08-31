package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.SortedSet;

public abstract class AbstractDomainGroup implements DomainGroup {
  private final Coordinator coord;

  protected AbstractDomainGroup(Coordinator coord) {
    this.coord = coord;
  }

  @Override
  public DomainGroupVersion getLatestVersion() throws IOException {
    SortedSet<DomainGroupVersion> versions = getVersions();
    if (versions == null || versions.size() == 0) {
      return null;
    } else {
      return versions.last();
    }
  }

  @Override
  public int compareTo(DomainGroup o) {
    return getName().compareTo(o.getName());
  }

  protected Coordinator getCoord() {
    return coord;
  }

  @Override
  public void removeDomainFromAllVersions(Domain domain) throws IOException {
    for (DomainGroupVersion dgv : getVersions()) {
      dgv.removeDomain(domain);
    }
  }
}
