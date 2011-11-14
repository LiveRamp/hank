package com.rapleaf.hank.storage.curly;

import com.rapleaf.hank.storage.Deleter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class CurlyDeleter implements Deleter {
  private final String localPartitionRoot;

  public CurlyDeleter(String localPartitionRoot) {
    this.localPartitionRoot = localPartitionRoot;
  }

  @Override
  public void delete() throws IOException {
    FileUtils.deleteDirectory(new File(localPartitionRoot));
  }
}
