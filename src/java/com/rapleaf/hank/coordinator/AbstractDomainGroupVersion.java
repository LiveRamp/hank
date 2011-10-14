package com.rapleaf.hank.coordinator;

import java.io.IOException;


public abstract class AbstractDomainGroupVersion implements DomainGroupVersion {

  @Override
  public DomainGroupVersionDomainVersion getDomainVersion(Domain domain) {
    if (domain == null || domain.getName() == null) {
      return null;
    }
    for (DomainGroupVersionDomainVersion domainVersion : getDomainVersions()) {
      if (domainVersion.getDomain() != null &&
          domain.getName().equals(domainVersion.getDomain().getName())) {
        return domainVersion;
      }
    }
    return null;
  }

  @Override
  public int compareTo(DomainGroupVersion o) {
    return Integer.valueOf(getVersionNumber()).compareTo(o.getVersionNumber());
  }

  @Override
  public boolean containsDomain(Domain domain) {
    for (DomainGroupVersionDomainVersion dgvdv : getDomainVersions()) {
      if (dgvdv.getDomain().equals(domain)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void removeDomain(Domain domain) throws IOException {
    for (DomainGroupVersionDomainVersion dgvdv : getDomainVersions()) {
      if (dgvdv.getDomain().equals(domain)) {
        dgvdv.delete();
      }
    }
  }

  @Override
  public String toString() {
    return "AbstractDomainGroupVersion [domainVersions=" + getDomainVersions() + ", domainGroup="
        + (getDomainGroup() != null ? getDomainGroup().getName() : "null") + ", versionNumber=" + getVersionNumber() + "]";
  }
}
