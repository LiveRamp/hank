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

  @Override
  public boolean isDomainRemovable(Domain domain) throws IOException {
    int domainId = getDomainId(domain.getName());
    for (RingGroup rg : coord.getRingGroups()) {
      if (rg.getDomainGroup().equals(this)) {
        for (Ring r : rg.getRings()) {
          for (Host h : r.getHosts()) {
            if (h.getDomainById(domainId).getPartitions().size() > 0) {
              return false;
            }
          }
        }
      }
    }
    return true;
  }

  protected Coordinator getCoord() {
    return coord;
  }
}
