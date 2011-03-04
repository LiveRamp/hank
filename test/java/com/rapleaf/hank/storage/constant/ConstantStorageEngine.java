package com.rapleaf.hank.storage.constant;

import java.io.IOException;
import java.util.Map;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.rapleaf.hank.config.PartservConfigurator;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;
import com.rapleaf.hank.storage.Updater;
import com.rapleaf.hank.storage.Writer;

public class ConstantStorageEngine implements StorageEngine {

  public static class Factory implements StorageEngineFactory {
    @Override
    public StorageEngine getStorageEngine(Map<String, Object> options)
        throws IOException {
      return new ConstantStorageEngine(options);
    }

  }

  public ConstantStorageEngine(Map<String, Object> options) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public Reader getReader(PartservConfigurator configurator, int partNum)
      throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public Updater getUpdater(PartservConfigurator configurator, int partNum) {
    throw new NotImplementedException();
  }

  @Override
  public Writer getWriter(OutputStreamFactory streamFactory, int partNum,
      int versionNumber, boolean base) throws IOException {
    throw new UnsupportedOperationException();
  }
}
