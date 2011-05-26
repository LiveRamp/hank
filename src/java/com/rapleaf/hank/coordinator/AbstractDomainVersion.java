package com.rapleaf.hank.coordinator;

import java.io.IOException;

public abstract class AbstractDomainVersion implements DomainVersion {

  public int compareTo(DomainVersion domainVersion) {
    return Integer.valueOf(getVersionNumber()).compareTo(domainVersion.getVersionNumber());
  }

  public final boolean isClosed() throws IOException {
    return getClosedAt() != null;
  }
}
