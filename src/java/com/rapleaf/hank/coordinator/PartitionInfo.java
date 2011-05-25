package com.rapleaf.hank.coordinator;

public interface PartitionInfo {
  public int getPartNum();

  public long getNumBytes();

  public long getNumRecords();
}
