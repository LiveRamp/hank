/**
 *  Copyright 2011 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.liveramp.hank.storage.constant;

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

public class ConstantStorageEngine implements StorageEngine {

  public static class Factory implements StorageEngineFactory {
    @Override
    public StorageEngine getStorageEngine(Map<String, Object> options, Domain domain) throws IOException {
      return new ConstantStorageEngine(options);
    }

    @Override
    public String getPrettyName() {
      return "Constant";
    }

    @Override
    public String getDefaultOptions() {
      return "---";
    }
  }

  public ConstantStorageEngine(Map<String, Object> options) {
  }

  @Override
  public Reader getReader(ReaderConfigurator configurator, int partitionNumber, DiskPartitionAssignment assignment) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PartitionUpdater getUpdater(DiskPartitionAssignment configurator, int partitionNumber) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Compactor getCompactor(DiskPartitionAssignment configurator,
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
  public Writer getWriter(DomainVersion domainVersion,
                          PartitionRemoteFileOps partitionRemoteFileOps,
                          int partitionNumber) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Deleter getDeleter(DiskPartitionAssignment configurator, int partitionNumber)
      throws IOException {
    throw new UnsupportedOperationException();
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
  public RemoteDomainVersionDeleter getRemoteDomainVersionDeleter(RemoteLocation location) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public RemoteDomainCleaner getRemoteDomainCleaner() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DiskPartitionAssignment getDataDirectoryPerPartition(DataDirectoriesConfigurator configurator, Collection<Integer> partitionNumbers) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getFiles(DiskPartitionAssignment configurator, int versionNumber, int partitionNumber) throws IOException {
    return Collections.emptySet();
  }
}
