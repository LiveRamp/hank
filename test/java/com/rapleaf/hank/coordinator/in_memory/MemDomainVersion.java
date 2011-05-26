package com.rapleaf.hank.coordinator.in_memory;

import com.rapleaf.hank.coordinator.AbstractDomainVersion;
import com.rapleaf.hank.coordinator.PartitionInfo;

import java.util.Set;

public class MemDomainVersion extends AbstractDomainVersion {
  private final int versionNumber;
  private final Long closedAt;

  public MemDomainVersion(int versionNumber, Long closedAt) {
    this.versionNumber = versionNumber;
    this.closedAt = closedAt;
  }

  public Long getClosedAt() {
    return closedAt;
  }

  public int getVersionNumber() {
    return versionNumber;
  }

  public void addPartitionInfo(int partNum, long numBytes, long numRecords) {
    // TODO Auto-generated method stub

  }

  public void cancel() {
    // TODO Auto-generated method stub

  }

  public void close() {
    // TODO Auto-generated method stub

  }

  public Set<PartitionInfo> getPartitionInfos() {
    // TODO Auto-generated method stub
    return null;
  }
}
