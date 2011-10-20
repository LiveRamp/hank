package com.rapleaf.hank.coordinator;

import java.io.IOException;

public abstract class AbstractRingGroup implements RingGroup {
  @Override
  public boolean isUpdating() throws IOException {
    return getUpdatingToVersion() != null;
  }

  public boolean isAssigned(DomainGroupVersion domainGroupVersion) throws IOException {
    for (Ring ring : getRings()) {
      if (!Rings.isAssigned(ring, domainGroupVersion)) {
        return false;
      }
    }
    return true;
  }

  public boolean isUpToDate(DomainGroupVersion domainGroupVersion) throws IOException {
    for (Ring ring : getRings()) {
      if (!Rings.isUpToDate(ring, domainGroupVersion)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    try {
      return "AbstractRingGroup [name=" + getName()
          + ", domain group=" + getDomainGroup()
          + ", current version=" + getCurrentVersion()
          + ", updating to version=" + getUpdatingToVersion()
          + "]";
    } catch (IOException e) {
      return "AbstractRingGroup [name=" + getName()
          + ", domain group=" + getDomainGroup()
          + ", current version=?"
          + ", updating to version=?"
          + "]";
    }
  }
}
