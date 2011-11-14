package com.rapleaf.hank.storage.cueball;

import java.io.File;
import java.io.IOException;

import com.rapleaf.hank.storage.Deleter;
import org.apache.commons.io.FileUtils;

public class CueballDeleter implements Deleter {
  private final String localPartitionRoot;

  public CueballDeleter(String localPartitionRoot) {
    this.localPartitionRoot = localPartitionRoot;
  }

  @Override
  public void delete() throws IOException {
    FileUtils.deleteDirectory(new File(localPartitionRoot));
  }
}
