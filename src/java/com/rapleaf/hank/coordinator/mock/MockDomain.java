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

import com.rapleaf.hank.coordinator.AbstractDomain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;

import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;

public class MockDomain extends AbstractDomain {
  private final String name;
  private final int numParts;
  private final Partitioner part;
  private final StorageEngine storageEngine;
  private DomainVersion version;

  public MockDomain(String name,
                    int numParts,
                    Partitioner part,
                    StorageEngine storageEngine,
                    DomainVersion version) {
    this.name = name;
    this.numParts = numParts;
    this.part = part;
    this.storageEngine = storageEngine;
    this.version = version;
  }

  public String getName() {
    return name;
  }

  public int getNumParts() {
    return numParts;
  }

  public Partitioner getPartitioner() {
    return part;
  }

  public StorageEngine getStorageEngine() {
    return storageEngine;
  }

  @Override
  public String toString() {
    return "MockDomainConfig [name=" + name + ", numParts=" + numParts
        + ", part=" + part + ", storageEngine=" + storageEngine + ", version="
        + version + "]";
  }

  public Class<? extends StorageEngineFactory> getStorageEngineFactoryClass() {
    return null;
  }

  public Map<String, Object> getStorageEngineOptions() {
    return null;
  }

  public SortedSet<DomainVersion> getVersions() {
    return null;
  }

  public DomainVersion openNewVersion() throws IOException {
    return version;
  }
}
