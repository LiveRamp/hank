package com.rapleaf.hank.coordinator;

public interface PartitionInfo {
  public int getPartitionNumber();

  public long getNumBytes();

  public long getNumRecords();
}
