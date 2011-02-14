package com.rapleaf.hank.storage;

import java.io.IOException;

import com.rapleaf.hank.config.PartDaemonConfigurator;
import com.rapleaf.hank.config.UpdateDaemonConfigurator;

public class MockStorageEngine implements StorageEngine {
  public boolean getReaderCalled;

  @Override
  public Reader getReader(PartDaemonConfigurator configurator, int partNum)
  throws IOException {
    getReaderCalled = true;
    return null;
  }

  @Override
  public Updater getUpdater(UpdateDaemonConfigurator configurator, int partNum) {
    return null;
  }

  @Override
  public Writer getWriter(OutputStreamFactory streamFactory, int partNum,
      int versionNumber, boolean base) throws IOException {
    return null;
  }
}
