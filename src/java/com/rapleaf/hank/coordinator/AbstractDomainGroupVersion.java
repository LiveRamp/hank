package com.rapleaf.hank.coordinator;


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
}
