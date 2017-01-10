package com.liveramp.hank.storage.echo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.liveramp.hank.config.DataDirectoriesConfigurator;
import com.liveramp.hank.config.ReaderConfigurator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.partition_server.DiskPartitionAssignment;
import com.liveramp.hank.storage.Compactor;
import com.liveramp.hank.storage.Deleter;
import com.liveramp.hank.storage.PartitionRemoteFileOps;
import com.liveramp.hank.storage.PartitionRemoteFileOpsFactory;
import com.liveramp.hank.storage.PartitionUpdater;
import com.liveramp.hank.storage.Reader;
import com.liveramp.hank.storage.RemoteDomainCleaner;
import com.liveramp.hank.storage.RemoteDomainVersionDeleter;
import com.liveramp.hank.storage.StorageEngine;
import com.liveramp.hank.storage.StorageEngineFactory;
import com.liveramp.hank.storage.Writer;

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
  public PartitionRemoteFileOpsFactory getPartitionRemoteFileOpsFactory(RemoteLocation location) {
    return null;
  }

  @Override
  public PartitionRemoteFileOps getPartitionRemoteFileOps(RemoteLocation location, int partitionNumber) throws IOException {
    return null;
  }

  @Override
  public Reader getReader(ReaderConfigurator configurator, int partitionNumber, DiskPartitionAssignment assignment) throws IOException {
    return new EchoReader(partitionNumber);
  }

  @Override
  public Writer getWriter(DomainVersion domainVersion,
                          PartitionRemoteFileOps partitionRemoteFileOps,
                          int partitionNumber) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PartitionUpdater getUpdater(DiskPartitionAssignment configurator, int partitionNumber) throws IOException {
    return new EchoUpdater();
  }

  @Override
  public Compactor getCompactor(DiskPartitionAssignment assignment,
                                int partitionNumber) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Writer getCompactorWriter(DomainVersion domainVersion,
                                   PartitionRemoteFileOps partitionRemoteFileOps,
                                   int partitionNumber) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Deleter getDeleter(DiskPartitionAssignment assignment, int partitionNumber)
      throws IOException {
    return new EchoDeleter(partitionNumber);
  }

  @Override
  public RemoteDomainVersionDeleter getRemoteDomainVersionDeleter(RemoteLocation location) throws IOException {
    return new RemoteDomainVersionDeleter() {
      @Override
      public void deleteVersion(int versionNumber) throws IOException {
      }
    };
  }

  @Override
  public RemoteDomainCleaner getRemoteDomainCleaner() throws IOException {
    return null;
  }

  @Override
  public DiskPartitionAssignment getDataDirectoryPerPartition(DataDirectoriesConfigurator configurator, Collection<Integer> partitionNumbers) {
    return null;
  }

  @Override
  public Set<String> getFiles(DiskPartitionAssignment assignment, int versionNumber, int partitionNumber) throws IOException {
    return Collections.emptySet();
  }
}
