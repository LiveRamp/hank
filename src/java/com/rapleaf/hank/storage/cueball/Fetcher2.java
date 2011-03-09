package com.rapleaf.hank.storage.cueball;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public final class Fetcher2 implements IFetcher {
  private static final Logger LOG = Logger.getLogger(Fetcher2.class);

  private final IFileOps fileOps;
  private final IFileSelector fileSelector;

  public Fetcher2(IFileOps fileOps, IFileSelector fileSelector) {
    this.fileOps = fileOps;
    this.fileSelector = fileSelector;
  }

  @Override
  public void fetch(int fromVersion, int toVersion) throws IOException {
    List<String> remoteFiles = fileOps.listFiles();
    LOG.debug("Remote files: " + remoteFiles);

    List<String> relevantFiles = new ArrayList(remoteFiles.size());
    for (String fileName : remoteFiles) {
      if (fileSelector.isRelevantFile(fileName, fromVersion, toVersion)) {
        relevantFiles.add(fileName);
      }
    }
    LOG.debug("Relevant files: " + relevantFiles);

    List<String> filesToCopy = fileSelector.selectFilesToCopy(relevantFiles, fromVersion, toVersion);

    for (String fileName : filesToCopy) {
      LOG.debug("Copying " + fileName + " to local");
      fileOps.copyToLocal(fileName);
    }
  }
}
