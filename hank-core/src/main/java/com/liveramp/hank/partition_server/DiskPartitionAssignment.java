package com.liveramp.hank.partition_server;

import java.util.Map;

public class DiskPartitionAssignment {

  private final Map<Integer, String> dataDirectoryPerPartition;

  public DiskPartitionAssignment(Map<Integer, String> dataDirectoryPerPartition) {
    this.dataDirectoryPerPartition = dataDirectoryPerPartition;
  }

  public String getDisk(int partitionNumber) {
    return dataDirectoryPerPartition.get(partitionNumber);
  }
}
