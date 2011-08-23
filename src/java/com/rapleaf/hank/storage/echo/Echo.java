package com.rapleaf.hank.storage.echo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import com.rapleaf.hank.config.PartitionServerConfigurator;
import com.rapleaf.hank.storage.Deleter;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;
import com.rapleaf.hank.storage.Updater;
import com.rapleaf.hank.storage.Writer;

public class Echo implements StorageEngine {
  public static class Factory implements StorageEngineFactory {
    @Override
    public String getDefaultOptions() {
      return "---\n# This storage engine doesn't take any options!";
    }

    @Override
    public String getPrettyName() {
      return "Echo";
    }

    @Override
    public StorageEngine getStorageEngine(Map<String, Object> options, String domainName) throws IOException {
      return new Echo();
    }
  }

  @Override
  public ByteBuffer getComparableKey(ByteBuffer key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Reader getReader(PartitionServerConfigurator configurator, int partNum) throws IOException {
    return new EchoReader(partNum);
  }

  @Override
  public Updater getUpdater(PartitionServerConfigurator configurator, int partNum) throws IOException {
    return new EchoUpdater();
  }

  @Override
  public Writer getWriter(OutputStreamFactory streamFactory, int partNum, int versionNumber, boolean base) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Deleter getDeleter(PartitionServerConfigurator configurator, int partNum)
      throws IOException {
    return new EchoDeleter(partNum);
  }
}
