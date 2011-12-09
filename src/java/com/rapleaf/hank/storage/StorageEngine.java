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

package com.rapleaf.hank.storage;

import com.rapleaf.hank.config.CoordinatorConfigurator;
import com.rapleaf.hank.config.DataDirectoriesConfigurator;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Defines how to read, write, delete, and update the data stored for a given
 * partition.
 */
public interface StorageEngine {

  public Reader getReader(DataDirectoriesConfigurator configurator, int partitionNumber) throws IOException;

  public Writer getWriter(OutputStreamFactory streamFactory, int partitionNumber, int versionNumber, boolean isBase)
      throws IOException;

  public PartitionUpdater getUpdater(DataDirectoriesConfigurator configurator, int partitionNumber)
      throws IOException;

  public Compactor getCompactor(DataDirectoriesConfigurator configurator,
                                OutputStreamFactory outputStreamFactory,
                                int partitionNumber) throws IOException;

  public Deleter getDeleter(DataDirectoriesConfigurator configurator, int partitionNumber) throws IOException;

  public ByteBuffer getComparableKey(ByteBuffer key);

  public DomainVersionCleaner getDomainVersionCleaner(CoordinatorConfigurator configurator) throws IOException;
}
