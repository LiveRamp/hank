package com.rapleaf.hank.storage;

import java.io.IOException;

public interface RemoteDomainVersionCleaner {
  /**
   * Purge the specified version from remote storage.
   *
   * @param versionNumber
   * @throws IOException
   */
  public void cleanVersion(int versionNumber) throws IOException;
}
