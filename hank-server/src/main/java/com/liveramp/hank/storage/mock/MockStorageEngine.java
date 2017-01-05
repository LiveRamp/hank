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
package com.liveramp.hank.storage.mock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import com.liveramp.hank.config.DataDirectoriesConfigurator;
import com.liveramp.hank.config.ReaderConfigurator;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.partition_server.DiskPartitionAssignment;
import com.liveramp.hank.storage.Compactor;
import com.liveramp.hank.storage.Deleter;
import com.liveramp.hank.storage.HdfsPartitionRemoteFileOps;
import com.liveramp.hank.storage.PartitionRemoteFileOps;
import com.liveramp.hank.storage.PartitionRemoteFileOpsFactory;
import com.liveramp.hank.storage.PartitionUpdater;
import com.liveramp.hank.storage.Reader;
import com.liveramp.hank.storage.RemoteDomainCleaner;
import com.liveramp.hank.storage.RemoteDomainVersionDeleter;
import com.liveramp.hank.storage.StorageEngine;
import com.liveramp.hank.storage.Writer;

public class MockStorageEngine implements StorageEngine {

  public boolean getReaderCalled;

  @Override
  public Reader getReader(ReaderConfigurator configurator, int partitionNumber, DiskPartitionAssignment assignment)
      throws IOException {
    getReaderCalled = true;
    return null;
  }

  @Override
  public Writer getWriter(DomainVersion domainVersion,
                          PartitionRemoteFileOps partitionRemoteFileOps,
                          int partitionNumber) throws IOException {
    return null;
  }

  @Override
  public PartitionUpdater getUpdater(DiskPartitionAssignment assignment, int partitionNumber) {
    return null;
  }

  @Override
  public Compactor getCompactor(DiskPartitionAssignment assignment,
                                int partitionNumber) throws IOException {
    return null;
  }

  @Override
  public Writer getCompactorWriter(DomainVersion domainVersion,
                                   PartitionRemoteFileOps partitionRemoteFileOps,
                                   int partitionNumber) throws IOException {
    return null;
  }

  @Override
  public Deleter getDeleter(DiskPartitionAssignment assignment, int partitionNumber)
      throws IOException {
    return new MockDeleter(partitionNumber);
  }

  @Override
  public ByteBuffer getComparableKey(ByteBuffer key) {
    return null;
  }

  @Override
  public PartitionRemoteFileOpsFactory getPartitionRemoteFileOpsFactory(RemoteLocation location) {
    return new HdfsPartitionRemoteFileOps.Factory();
  }

  @Override
  public PartitionRemoteFileOps getPartitionRemoteFileOps(RemoteLocation location, int partitionNumber) {
    return null;
  }

  @Override
  public RemoteDomainVersionDeleter getRemoteDomainVersionDeleter(RemoteLocation location) throws IOException {
    return null;
  }

  @Override
  public RemoteDomainCleaner getRemoteDomainCleaner() throws IOException {
    return null;
  }

  @Override
  public DiskPartitionAssignment getDataDirectoryPerPartition(DataDirectoriesConfigurator configurator, Collection<Integer> partitionNumbers) {
    return new DiskPartitionAssignment(Collections.<Integer, String>emptyMap());
  }

  @Override
  public Set<String> getFiles(DiskPartitionAssignment assignment, int versionNumber, int partitionNumber) throws IOException {
    return Collections.emptySet();
  }
}
