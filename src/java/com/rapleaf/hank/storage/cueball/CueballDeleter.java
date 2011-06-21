package com.rapleaf.hank.storage.cueball;

import java.io.File;
import java.io.IOException;

import com.rapleaf.hank.storage.Deleter;

public class CueballDeleter implements Deleter {
  private final String localPartitionRoot;
  
  public CueballDeleter(String localPartitionRoot) {
    this.localPartitionRoot = localPartitionRoot;
  }
  
  @Override
  public void delete() throws IOException {
    if (!new File(localPartitionRoot).delete())
      throw new IOException();
  }
}
