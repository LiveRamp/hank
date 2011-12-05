package com.rapleaf.hank.storage.echo;

import com.rapleaf.hank.config.CoordinatorConfigurator;
import com.rapleaf.hank.config.PartitionServerConfigurator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.storage.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

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
    public StorageEngine getStorageEngine(Map<String, Object> options, Domain domain) throws IOException {
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
  public PartitionUpdater getUpdater(PartitionServerConfigurator configurator, int partNum) throws IOException {
    return new EchoUpdater();
  }

  @Override
  public PartitionUpdater getCompactingUpdater(PartitionServerConfigurator configurator, int partNum) throws IOException {
    throw new UnsupportedOperationException();
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

  @Override
  public DomainVersionCleaner getDomainVersionCleaner(CoordinatorConfigurator configurator) throws IOException {
    return new DomainVersionCleaner() {
      @Override
      public void cleanVersion(int versionNumber) throws IOException {
      }
    };
  }
}
