package com.rapleaf.hank.coordinator;

import java.io.IOException;

public abstract class AbstractDomainVersion implements DomainVersion {
  @Override
  public int compareTo(DomainVersion arg0) {
    return Integer.valueOf(getVersionNumber()).compareTo(arg0.getVersionNumber());
  }

  @Override
  public final boolean isClosed() throws IOException {
    return getClosedAt() != null;
  }
}
