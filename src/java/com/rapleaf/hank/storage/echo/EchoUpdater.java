package com.rapleaf.hank.storage.echo;

import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.storage.PartitionUpdater;

import java.io.IOException;

public class EchoUpdater implements PartitionUpdater {

  @Override
  public void updateTo(DomainVersion updatingToVersion) throws IOException {
    // No-op
  }
}
