package com.rapleaf.hank.coordinator;

import java.io.IOException;

public abstract class AbstractRingGroup implements RingGroup {
  @Override
  public boolean isUpdating() throws IOException {
    return getUpdatingToVersion() != null;
  }

  public boolean isAssigned(DomainGroupVersion domainGroupVersion) throws IOException {
    for (Ring ring : getRings()) {
      if (!ring.isAssigned(domainGroupVersion)) {
        return false;
      }
    }
    return true;
  }

  public boolean isUpToDate(DomainGroupVersion domainGroupVersion) throws IOException {
    for (Ring ring : getRings()) {
      if (!ring.isUpToDate(domainGroupVersion)) {
        return false;
      }
    }
    return true;
  }
}
