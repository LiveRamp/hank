/**
 *  Copyright 2011 Rapleaf
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
package com.rapleaf.hank.storage.mock;

import com.rapleaf.hank.config.DataDirectoriesConfigurator;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.DomainVersionPropertiesSerialization;
import com.rapleaf.hank.storage.*;

import java.io.IOException;
import java.nio.ByteBuffer;

public class MockStorageEngine implements StorageEngine {

  public boolean getReaderCalled;

  @Override
  public Reader getReader(DataDirectoriesConfigurator configurator, int partitionNumber)
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
  public RemoteDomainVersionDeleter getRemoteDomainVersionDeleter() throws IOException {
    return null;
  }

  @Override
  public RemoteDomainCleaner getRemoteDomainCleaner() throws IOException {
    return null;
  }

  @Override
  public DomainVersionPropertiesSerialization getDomainVersionPropertiesSerialization() {
    return null;
  }
}
