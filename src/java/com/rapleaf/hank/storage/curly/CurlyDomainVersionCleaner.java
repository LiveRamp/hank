package com.rapleaf.hank.storage.curly;

import java.io.IOException;

import com.rapleaf.hank.storage.DomainVersionCleaner;
import com.rapleaf.hank.storage.IFileOps;
import com.rapleaf.hank.storage.cueball.CueballDomainVersionCleaner;

public class CurlyDomainVersionCleaner extends CueballDomainVersionCleaner implements DomainVersionCleaner {
  public CurlyDomainVersionCleaner(String domainVersionsRoot, IFileOps fs) {
    super(domainVersionsRoot, fs);
  }

  @Override
  public void cleanVersion(int versionNumber, int numParts) throws IOException {
    super.cleanVersion(versionNumber, numParts);
    for (int i = 0; i < numParts; i++) {
      String basePath = domainVersionsRoot + "/" + i + "/" + Curly.padVersionNumber(versionNumber) + ".base.curly";
      String deltaPath = domainVersionsRoot + "/" + i + "/" + Curly.padVersionNumber(versionNumber) + ".delta.curly";
      if (!fs.attemptDeleteRemote(basePath)) {
        if (!fs.attemptDeleteRemote(deltaPath)){
          // TODO: log a note that no file was found.
        }
      }
    }
  }
}
