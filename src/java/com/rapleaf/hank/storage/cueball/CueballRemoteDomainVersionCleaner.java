package com.rapleaf.hank.storage.cueball;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.storage.PartitionRemoteFileOps;
import com.rapleaf.hank.storage.PartitionRemoteFileOpsFactory;
import com.rapleaf.hank.storage.RemoteDomainVersionCleaner;

import java.io.IOException;

public class CueballRemoteDomainVersionCleaner implements RemoteDomainVersionCleaner {

  protected final Domain domain;
  protected final String remoteDomainRoot;
  protected final PartitionRemoteFileOpsFactory fileOpsFactory;

  public CueballRemoteDomainVersionCleaner(Domain domain,
                                           String remoteDomainRoot,
                                           PartitionRemoteFileOpsFactory fileOpsFactory) {
    this.domain = domain;
    this.remoteDomainRoot = remoteDomainRoot;
    this.fileOpsFactory = fileOpsFactory;
  }

  @Override
  public void cleanVersion(int versionNumber) throws IOException {
    for (int partition = 0; partition < domain.getNumParts(); ++partition) {
      PartitionRemoteFileOps fileOps = fileOpsFactory.getFileOps(remoteDomainRoot, partition);
      fileOps.attemptDelete(Cueball.getName(versionNumber, true));
      fileOps.attemptDelete(Cueball.getName(versionNumber, false));
    }
  }
}
