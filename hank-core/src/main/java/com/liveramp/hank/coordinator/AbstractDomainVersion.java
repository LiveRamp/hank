package com.liveramp.hank.coordinator;

import java.io.IOException;

public abstract class AbstractDomainVersion implements DomainVersion {

  public int compareTo(DomainVersion domainVersion) {
    return Integer.valueOf(getVersionNumber()).compareTo(domainVersion.getVersionNumber());
  }

  @Override
  public String toString() {
    Boolean isClosed;
    Boolean isDefunct;
    try {
      isClosed = DomainVersions.isClosed(this);
    } catch (IOException e) {
      isClosed = null;
    }
    try {
      isDefunct = isDefunct();
    } catch (IOException e) {
      isDefunct = null;
    }
    return "AbstractDomainVersion ["
        + "version=" + getVersionNumber()
        + ", closed=" + isClosed
        + ", defunct=" + isDefunct
        + "]";
  }
}
