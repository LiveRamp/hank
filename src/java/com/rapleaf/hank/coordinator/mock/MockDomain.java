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
package com.rapleaf.hank.coordinator.mock;

import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersionConfig;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;

public class MockDomain implements Domain {
  private final String name;
  private final int numParts;
  private final Partitioner part;
  private final StorageEngine storageEngine;
  private int version;

  public MockDomain(String name, int numParts, Partitioner part,
      StorageEngine storageEngine, int version) {
    this.name = name;
    this.numParts = numParts;
    this.part = part;
    this.storageEngine = storageEngine;
    this.version = version;
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
  public StorageEngine getStorageEngine() {
    return storageEngine;
  }

  @Override
  public String toString() {
    return "MockDomainConfig [name=" + name + ", numParts=" + numParts
    + ", part=" + part + ", storageEngine=" + storageEngine + ", version="
    + version + "]";
  }

  @Override
  public Class<? extends StorageEngineFactory> getStorageEngineFactoryClass() {
    return null;
  }

  @Override
  public Map<String, Object> getStorageEngineOptions() {
    return null;
  }

  @Override
  public void cancelNewVersion() throws IOException {
  }

  @Override
  public boolean closeNewVersion() throws IOException {
    return true;
  }

  @Override
  public SortedSet<DomainVersionConfig> getVersions() {
    return null;
  }

  @Override
  public Integer getOpenVersionNumber() {
    return version;
  }

  @Override
  public Integer openNewVersion() throws IOException {
    return version;
  }
}
