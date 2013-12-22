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

package com.liveramp.hank.storage;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.liveramp.hank.config.DataDirectoriesConfigurator;
import com.liveramp.hank.config.ReaderConfigurator;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.DomainVersionPropertiesSerialization;

/**
 * Defines how to read, write, delete, and update the data stored for a given
 * partition.
 */
public interface StorageEngine {

  public Reader getReader(ReaderConfigurator configurator, int partitionNumber) throws IOException;

  public Writer getWriter(DomainVersion domainVersion,
                          PartitionRemoteFileOps partitionRemoteFileOps,
                          int partitionNumber) throws IOException;

  public PartitionUpdater getUpdater(DataDirectoriesConfigurator configurator, int partitionNumber) throws IOException;

  public Compactor getCompactor(DataDirectoriesConfigurator configurator,
                                int partitionNumber) throws IOException;

  public Writer getCompactorWriter(DomainVersion domainVersion,
                                   PartitionRemoteFileOps partitionRemoteFileOps,
                                   int partitionNumber) throws IOException;

  public Deleter getDeleter(DataDirectoriesConfigurator configurator, int partitionNumber) throws IOException;

  public ByteBuffer getComparableKey(ByteBuffer key);

  public PartitionRemoteFileOpsFactory getPartitionRemoteFileOpsFactory();

  public PartitionRemoteFileOps getPartitionRemoteFileOps(int partitionNumber) throws IOException;

  public RemoteDomainVersionDeleter getRemoteDomainVersionDeleter() throws IOException;

  public RemoteDomainCleaner getRemoteDomainCleaner() throws IOException;

  public DomainVersionPropertiesSerialization getDomainVersionPropertiesSerialization();

  public String getDataDirectory(DataDirectoriesConfigurator configurator, int partitionNumber);
}
