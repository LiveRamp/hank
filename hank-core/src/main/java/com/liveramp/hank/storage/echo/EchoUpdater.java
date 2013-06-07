package com.liveramp.hank.storage.echo;

import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.partition_server.PartitionUpdateTaskStatistics;
import com.liveramp.hank.storage.PartitionUpdater;

import java.io.IOException;

public class EchoUpdater implements PartitionUpdater {

  @Override
  public void updateTo(DomainVersion updatingToVersion, PartitionUpdateTaskStatistics statistics) throws IOException {
    // No-op
  }
}
