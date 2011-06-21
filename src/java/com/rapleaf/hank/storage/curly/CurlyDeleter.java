package com.rapleaf.hank.storage.curly;

import java.io.File;
import java.io.IOException;

import com.rapleaf.hank.storage.Deleter;

public class CurlyDeleter implements Deleter {
  private final String localPartitionRoot;
  
  public CurlyDeleter(String localPartitionRoot) {
    this.localPartitionRoot = localPartitionRoot;
  }

  @Override
  public void delete() throws IOException {
    if (!new File(localPartitionRoot).delete())
      throw new IOException("Failed to delete partition at " + localPartitionRoot);
  }
}
