package com.rapleaf.hank.coordinator;

import java.io.IOException;

public abstract class AbstractDomainVersion implements DomainVersion {
  public int compareTo(DomainVersion domainVersion) {
    return Integer.valueOf(getVersionNumber()).compareTo(domainVersion.getVersionNumber());
  }

  public final boolean isClosed() throws IOException {
    return getClosedAt() != null;
  }

  @Override
  public long getTotalNumBytes() throws IOException {
    long total = 0;
    for (PartitionInfo pi : getPartitionInfos()) {
      total += pi.getNumBytes();
    }
    return total;
  }

  @Override
  public long getTotalNumRecords() throws IOException {
    long total = 0;
    for (PartitionInfo pi : getPartitionInfos()) {
      total += pi.getNumRecords();
    }
    return total;
  }

  @Override
  public String toString() {
    Boolean isClosed;
    Boolean isDefunct;
    try {
      isClosed = isClosed();
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
          + "closed=" + isClosed
          + "defunct=" + isDefunct
          + "]";
  }
}
