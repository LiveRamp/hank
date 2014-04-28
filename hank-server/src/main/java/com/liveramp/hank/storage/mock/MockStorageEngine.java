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
import java.util.Collections;
import java.util.Set;

import com.liveramp.hank.config.DataDirectoriesConfigurator;
import com.liveramp.hank.config.ReaderConfigurator;
import com.liveramp.hank.coordinator.DomainVersion;
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
  public Reader getReader(ReaderConfigurator configurator, int partitionNumber)
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
  public PartitionUpdater getUpdater(DataDirectoriesConfigurator configurator, int partitionNumber) {
    return null;
  }

  @Override
  public Compactor getCompactor(DataDirectoriesConfigurator configurator,
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
  public Deleter getDeleter(DataDirectoriesConfigurator configurator, int partitionNumber)
      throws IOException {
    return new MockDeleter(partitionNumber);
  }

  @Override
  public ByteBuffer getComparableKey(ByteBuffer key) {
    return null;
  }

  @Override
  public PartitionRemoteFileOpsFactory getPartitionRemoteFileOpsFactory() {
    return new HdfsPartitionRemoteFileOps.Factory();
  }

  @Override
  public PartitionRemoteFileOps getPartitionRemoteFileOps(int partitionNumber) {
    return null;
  }

  @Override
  public RemoteDomainVersionDeleter getRemoteDomainVersionDeleter() throws IOException {
    return null;
  }

  @Override
  public RemoteDomainCleaner getRemoteDomainCleaner() throws IOException {
    return null;
  }

  @Override
  public String getDataDirectory(DataDirectoriesConfigurator configurator, int partitionNumber) {
    return null;
  }

  @Override
  public Set<String> getFiles(DataDirectoriesConfigurator configurator, int versionNumber, int partitionNumber) throws IOException {
    return Collections.emptySet();
  }
}
