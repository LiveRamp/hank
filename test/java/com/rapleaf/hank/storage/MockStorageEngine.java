package com.rapleaf.hank.storage;

import java.io.IOException;

import com.rapleaf.hank.config.PartservConfigurator;

public class MockStorageEngine implements StorageEngine {
  public boolean getReaderCalled;

  @Override
  public Reader getReader(PartservConfigurator configurator, int partNum)
  throws IOException {
    getReaderCalled = true;
    return null;
  }

  @Override
  public Updater getUpdater(PartservConfigurator configurator, int partNum) {
    return null;
  }

  @Override
  public Writer getWriter(OutputStreamFactory streamFactory, int partNum,
      int versionNumber, boolean base) throws IOException {
    return null;
  }
}
