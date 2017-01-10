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
import java.util.Collection;
import java.util.Set;

import com.liveramp.hank.config.DataDirectoriesConfigurator;
import com.liveramp.hank.config.ReaderConfigurator;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.partition_server.DiskPartitionAssignment;

/**
 * Defines how to read, write, delete, and update the data stored for a given
 * partition.
 */
public interface StorageEngine {

  public Reader getReader(ReaderConfigurator configurator, int partitionNumber, DiskPartitionAssignment assignment) throws IOException;

  public Writer getWriter(DomainVersion domainVersion,
                          PartitionRemoteFileOps partitionRemoteFileOps,
                          int partitionNumber) throws IOException;

  public PartitionUpdater getUpdater(DiskPartitionAssignment assignment, int partitionNumber) throws IOException;

  public Compactor getCompactor(DiskPartitionAssignment assignment,
                                int partitionNumber) throws IOException;

  public Writer getCompactorWriter(DomainVersion domainVersion,
                                   PartitionRemoteFileOps partitionRemoteFileOps,
                                   int partitionNumber) throws IOException;

  public Deleter getDeleter(DiskPartitionAssignment assignment, int partitionNumber) throws IOException;

  public ByteBuffer getComparableKey(ByteBuffer key);

  enum RemoteLocation {
    DOMAIN_BUILDER,
    PARTITION_SERVER
  }

  public PartitionRemoteFileOpsFactory getPartitionRemoteFileOpsFactory(RemoteLocation filesLocation);

  public PartitionRemoteFileOps getPartitionRemoteFileOps(RemoteLocation files, int partitionNumber) throws IOException;

  public RemoteDomainVersionDeleter getRemoteDomainVersionDeleter(RemoteLocation location) throws IOException;

  public RemoteDomainCleaner getRemoteDomainCleaner() throws IOException;

  public DiskPartitionAssignment getDataDirectoryPerPartition(DataDirectoriesConfigurator configurator, Collection<Integer> partitionNumbers);

  public Set<String> getFiles(DiskPartitionAssignment assignment, int versionNumber, int partitionNumber) throws IOException;
}
