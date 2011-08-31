package com.rapleaf.hank.coordinator;

import java.io.IOException;

public abstract class AbstractDomainGroup implements DomainGroup {
  private final Coordinator coord;

  protected AbstractDomainGroup(Coordinator coord) {
    this.coord = coord;
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
