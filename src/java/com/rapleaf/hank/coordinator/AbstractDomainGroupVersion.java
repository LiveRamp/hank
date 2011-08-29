package com.rapleaf.hank.coordinator;


public abstract class AbstractDomainGroupVersion implements DomainGroupVersion {

  @Override
  public DomainGroupVersionDomainVersion getDomainVersion(String domainName) {
    // TODO: implement domainVersions as a HashMap
    for (DomainGroupVersionDomainVersion domainVersion : getDomainVersions()) {
      if (domainName.equals(domainVersion.getDomain().getName())) {
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
