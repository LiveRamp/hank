package com.rapleaf.hank.storage.cueball;

import java.io.IOException;

import com.rapleaf.hank.storage.DomainVersionCleaner;

public class CueballDomainVersionCleaner implements DomainVersionCleaner {
  private final IFileOps fs;
  private final String domainVersionsRoot;

  public CueballDomainVersionCleaner(String domainVersionsRoot, IFileOps fs) {
    this.domainVersionsRoot = domainVersionsRoot;
    this.fs = fs;
  }

  @Override
  public void cleanVersion(int versionNumber, int numParts) throws IOException {
    for (int i = 0; i < numParts; i++) {
      String basePath = domainVersionsRoot + "/" + i + "/" + Cueball.padVersionNumber(versionNumber) + ".base.cueball";
      String deltaPath = domainVersionsRoot + "/" + i + "/" + Cueball.padVersionNumber(versionNumber) + ".delta.cueball";
      if (!fs.attemptDeleteRemote(basePath)) {
        if (!fs.attemptDeleteRemote(deltaPath)){
          // TODO: log a note that no file was found.
        }
      }
    }
  }
}
