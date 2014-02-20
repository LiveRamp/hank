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
package com.liveramp.hank.coordinator.mock;

import com.liveramp.hank.coordinator.AbstractDomain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.DomainVersionProperties;
import com.liveramp.hank.partitioner.Partitioner;
import com.liveramp.hank.storage.StorageEngine;
import com.liveramp.hank.storage.StorageEngineFactory;

import java.io.IOException;
import java.util.*;

public class MockDomain extends AbstractDomain {

  private final String name;
  private final int numParts;
  private final Partitioner part;
  private final StorageEngine storageEngine;
  private DomainVersion version;
  private final Map<String, Object> storageEngineOptions;
  private final int id;

  public MockDomain(String name,
                    int id,
                    int numParts,
                    Partitioner part,
                    StorageEngine storageEngine,
                    Map<String, Object> storageEngineOptions,
                    DomainVersion version) {
    this.name = name;
    this.id = id;
    this.numParts = numParts;
    this.part = part;
    this.storageEngine = storageEngine;
    this.version = version;
    this.storageEngineOptions = storageEngineOptions;
  }

  public MockDomain(String domainName) {
    this(domainName, 0, 1, null, null, null, null);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getNumParts() {
    return numParts;
  }

  @Override
  public Partitioner getPartitioner() {
    return part;
  }

  @Override
  public String getPartitionerClassName() {
    return part.getClass().getName();
  }

  @Override
  public List<String> getRequiredHostFlags() {
    return Collections.emptyList();
  }

  @Override
  public StorageEngine getStorageEngine() {
    return storageEngine;
  }

  @Override
  public String toString() {
    return "MockDomain [id=" + getId() + ", name=" + name + ", numParts=" + numParts
        + ", part=" + part + ", storageEngine=" + storageEngine + ", version="
        + version + "]";
  }

  @Override
  public Class<? extends StorageEngineFactory> getStorageEngineFactoryClass() {
    return null;
  }

  @Override
  public String getStorageEngineFactoryClassName() {
    return null;
  }

  @Override
  public Map<String, Object> getStorageEngineOptions() {
    return storageEngineOptions;
  }

  @Override
  public SortedSet<DomainVersion> getVersions() {
    SortedSet<DomainVersion> versions = new TreeSet<DomainVersion>();
    versions.add(version);
    return versions;
  }

  @Override
  public DomainVersion openNewVersion(DomainVersionProperties domainVersionProperties) throws IOException {
    return version;
  }

  @Override
  public DomainVersion getVersion(int versionNumber) throws IOException {
    return findVersion(getVersions(), versionNumber);
  }

  @Override
  public DomainVersion getVersionShallow(int versionNumber) throws IOException {
    return getVersion(versionNumber);
  }

  @Override
  public boolean deleteVersion(int versionNumber) throws IOException {
    return false;
  }

  @Override
  public int getId() {
    return id;
  }
}
