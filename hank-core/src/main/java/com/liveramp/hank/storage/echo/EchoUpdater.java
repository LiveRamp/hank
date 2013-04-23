package com.liveramp.hank.storage.echo;

import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.storage.PartitionUpdater;

import java.io.IOException;

public class EchoUpdater implements PartitionUpdater {

  @Override
  public void updateTo(DomainVersion updatingToVersion) throws IOException {
    // No-op
  }
}
