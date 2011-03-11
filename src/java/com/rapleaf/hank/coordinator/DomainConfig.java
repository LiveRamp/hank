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

import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.StorageEngine;

/**
 * Encapsulates all the information about a given Domain.
 */
public interface DomainConfig {
  public String getName();

  /**
   * The number of partitions this domain is configured for.
   * @return
   */
  public int getNumParts();

  public StorageEngine getStorageEngine();

  public Partitioner getPartitioner();

  /**
   * Get the latest version number.
   * @return
   */
  public int getVersion();

  /**
   * Increment and return the version number.
   * @return
   * @throws IOException
   */
  public int newVersion() throws IOException;
}
