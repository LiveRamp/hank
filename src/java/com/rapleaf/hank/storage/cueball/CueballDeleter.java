package com.rapleaf.hank.storage.cueball;

import java.io.File;

import com.rapleaf.hank.storage.Deleter;

public class CueballDeleter implements Deleter {
  private final String localPartitionRoot;
  
  public CueballDeleter(String localPartitionRoot) {
    this.localPartitionRoot = localPartitionRoot;
  }
  
  @Override
  public void delete() {
    new File(localPartitionRoot).delete();
  }
}
