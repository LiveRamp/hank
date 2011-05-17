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

import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;

import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;

/**
 * Encapsulates all the information about a given Domain.
 */
public interface DomainConfig {
  public String getName();

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
  public SortedSet<DomainVersionConfig> getVersions() throws IOException;

  /**
   * Attempt to open a new version of this domain. If there isn't another
   * version already open, the return value is the next version number that
   * should be used. If there is another version open, then the return value is
   * null, indicating that another writer has the version lock and you should
   * try again later.
   * 
   * @return
   */
  public Integer openNewVersion() throws IOException;

  /**
   * Attempt to close the currently open version.
   * 
   * @return true is the close is successful, false if there is no open version.
   */
  public boolean closeNewVersion() throws IOException;

  /**
   * If there is an open version, then cancel it, returning the version number
   * to the pool. If there is no open version, then this is a no-op.
   */
  public void cancelNewVersion() throws IOException;

  /**
   * Returns true if a new version is currently open.
   * 
   * @return
   * @throws IOException 
   */
  public Integer getOpenVersionNumber() throws IOException;
}
