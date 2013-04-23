package com.liveramp.hank.storage;

import java.io.IOException;

public interface RemoteDomainVersionDeleter {
  /**
   * Purge the specified version from remote storage.
   *
   * @param versionNumber
   * @throws IOException
   */
  public void deleteVersion(int versionNumber) throws IOException;
}
