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
package com.liveramp.hank.coordinator;

import com.liveramp.hank.partitioner.Partitioner;
import com.liveramp.hank.storage.StorageEngine;
import com.liveramp.hank.storage.StorageEngineFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

public interface Domain extends Comparable<Domain> {

  public String getName();

  public int getId();

  public int getNumParts();

  public Class<? extends StorageEngineFactory> getStorageEngineFactoryClass();

  public String getStorageEngineFactoryClassName();

  public StorageEngine getStorageEngine();

  public Map<String, Object> getStorageEngineOptions();

  public Partitioner getPartitioner();

  public String getPartitionerClassName();

  public List<String> getRequiredHostFlags();

  public SortedSet<DomainVersion> getVersions() throws IOException;

  public DomainVersion openNewVersion(DomainVersionProperties domainVersionProperties) throws IOException;

  public DomainVersion getVersion(int versionNumber) throws IOException;

  public DomainVersion getVersionShallow(int versionNumber) throws IOException;

  public boolean deleteVersion(int versionNumber) throws IOException;
}
