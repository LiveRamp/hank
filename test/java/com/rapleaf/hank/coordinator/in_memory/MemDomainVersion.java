package com.rapleaf.hank.coordinator.in_memory;

import java.util.Set;

import com.rapleaf.hank.coordinator.AbstractDomainVersion;
import com.rapleaf.hank.coordinator.PartitionInfo;

public class MemDomainVersion extends AbstractDomainVersion {
  private final int ver;

  public MemDomainVersion(int ver) {
    this.ver = ver;
  }

  @Override
  public Long getClosedAt() {
    // TODO Auto-generated method stub
    return 0L;
  }

  @Override
  public int getVersionNumber() {
    return ver;
  }

  @Override
  public void addPartitionInfo(int partNum, long numBytes, long numRecords) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void cancel() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Set<PartitionInfo> getPartitionInfos() {
    // TODO Auto-generated method stub
    return null;
  }
}
