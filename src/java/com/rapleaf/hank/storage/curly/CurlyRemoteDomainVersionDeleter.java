package com.rapleaf.hank.storage.curly;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.storage.PartitionRemoteFileOps;
import com.rapleaf.hank.storage.PartitionRemoteFileOpsFactory;
import com.rapleaf.hank.storage.RemoteDomainVersionDeleter;
import com.rapleaf.hank.storage.cueball.Cueball;

import java.io.IOException;

public class CurlyRemoteDomainVersionDeleter implements RemoteDomainVersionDeleter {

  protected final Domain domain;
  protected final String remoteDomainRoot;
  protected final PartitionRemoteFileOpsFactory fileOpsFactory;

  public CurlyRemoteDomainVersionDeleter(Domain domain,
                                         String remoteDomainRoot,
                                         PartitionRemoteFileOpsFactory fileOpsFactory) {
    this.domain = domain;
    this.remoteDomainRoot = remoteDomainRoot;
    this.fileOpsFactory = fileOpsFactory;
  }

  @Override
  public void deleteVersion(int versionNumber) throws IOException {
    for (int partition = 0; partition < domain.getNumParts(); ++partition) {
      PartitionRemoteFileOps fileOps = fileOpsFactory.getPartitionRemoteFileOps(remoteDomainRoot, partition);
      fileOps.attemptDelete(Cueball.getName(versionNumber, true));
      fileOps.attemptDelete(Cueball.getName(versionNumber, false));

      fileOps.attemptDelete(Curly.getName(versionNumber, true));
      fileOps.attemptDelete(Curly.getName(versionNumber, false));
    }
  }
}
