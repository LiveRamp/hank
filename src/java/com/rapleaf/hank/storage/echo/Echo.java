package com.rapleaf.hank.storage.echo;

import com.rapleaf.hank.config.DataDirectoriesConfigurator;
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
  public Reader getReader(DataDirectoriesConfigurator configurator, int partitionNumber) throws IOException {
    return new EchoReader(partitionNumber);
  }

  @Override
  public PartitionUpdater getUpdater(DataDirectoriesConfigurator configurator, int partitionNumber) throws IOException {
    return new EchoUpdater();
  }

  @Override
  public Compactor getCompactor(DataDirectoriesConfigurator configurator,
                                int partitionNumber) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Writer getCompactorWriter(OutputStreamFactory outputStreamFactory,
                                   int partitionNumber,
                                   int versionNumber,
                                   boolean isBase) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Writer getWriter(OutputStreamFactory streamFactory, int partitionNumber, int versionNumber, boolean isBase) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Deleter getDeleter(DataDirectoriesConfigurator configurator, int partitionNumber)
      throws IOException {
    return new EchoDeleter(partitionNumber);
  }

  @Override
  public RemoteDomainVersionCleaner getRemoteDomainVersionCleaner() throws IOException {
    return new RemoteDomainVersionCleaner() {
      @Override
      public void cleanVersion(int versionNumber) throws IOException {
      }
    };
  }
}
