package com.liveramp.hank.coordinator;

public interface PartitionProperties {

  public int getPartitionNumber();

  public long getNumBytes();

  public long getNumRecords();
}
