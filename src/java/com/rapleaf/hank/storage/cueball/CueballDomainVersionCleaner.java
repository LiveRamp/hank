package com.rapleaf.hank.storage.cueball;

import com.rapleaf.hank.storage.DomainVersionCleaner;
import com.rapleaf.hank.storage.PartitionRemoteFileOps;
import com.rapleaf.hank.storage.PartitionRemoteFileOpsFactory;

import java.io.IOException;

public class CueballDomainVersionCleaner implements DomainVersionCleaner {

  protected final String remoteDomainRoot;
  protected final PartitionRemoteFileOpsFactory fileOpsFactory;

  public CueballDomainVersionCleaner(String remoteDomainRoot,
                                     PartitionRemoteFileOpsFactory fileOpsFactory) {
    this.remoteDomainRoot = remoteDomainRoot;
    this.fileOpsFactory = fileOpsFactory;
  }

  @Override
  public void cleanVersion(int versionNumber) throws IOException {
    PartitionRemoteFileOps fileOps = fileOpsFactory.getFileOps(remoteDomainRoot, versionNumber);
    fileOps.attemptDelete();
  }
}
