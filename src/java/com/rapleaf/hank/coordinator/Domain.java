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
package com.rapleaf.hank.coordinator;

import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;

import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;

/**
 * Encapsulates all the information about a given Domain.
 */
public interface Domain extends Comparable<Domain> {
  public String getName();

  public int getId();

  /**
   * The number of partitions this domain is configured for.
   *
   * @return
   */
  public int getNumParts();

  public Class<? extends StorageEngineFactory> getStorageEngineFactoryClass();

  public StorageEngine getStorageEngine();

  public Map<String, Object> getStorageEngineOptions();

  public Partitioner getPartitioner();

  /**
   * Returns the set of DomainVersionConfigs for this Domain in version-numbered
   * order.
   *
   * @return
   * @throws IOException
   */
  public SortedSet<DomainVersion> getVersions() throws IOException;

  /**
   * Attempt to open a new version of this domain. If there isn't another
   * version already open, the return value is a new DomainVersion with the next
   * available version number. If there is another version open, then the return
   * value is null, indicating that another writer has the version lock and you
   * should try again later.
   *
   * @return
   */
  public DomainVersion openNewVersion() throws IOException;

  public DomainVersion getVersionByNumber(int versionNumber) throws IOException;
}
