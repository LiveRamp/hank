/**
 *  Copyright 2013 LiveRamp
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

package com.liveramp.hank.config;

import java.util.Set;

public class BaseReaderConfigurator implements ReaderConfigurator {

  private final DataDirectoriesConfigurator dataDirectoriesConfigurator;
  private final long cacheNumBytesCapacity;
  private final long cacheNumItemsCapacity;
  private final int bufferReuseMaxSize;
  private final int numTotalPartitions;

  public BaseReaderConfigurator(DataDirectoriesConfigurator dataDirectoriesConfigurator,
                                long cacheNumBytesCapacity,
                                long cacheNumItemsCapacity,
                                int bufferReuseMaxSize,
                                int numTotalPartitions) {
    this.dataDirectoriesConfigurator = dataDirectoriesConfigurator;
    this.cacheNumBytesCapacity = cacheNumBytesCapacity;
    this.cacheNumItemsCapacity = cacheNumItemsCapacity;
    this.bufferReuseMaxSize = bufferReuseMaxSize;
    this.numTotalPartitions = numTotalPartitions;
  }

  @Override
  public long getCacheNumBytesCapacity() {
    if (cacheNumBytesCapacity < 0) {
      return cacheNumBytesCapacity;
    }
    return cacheNumBytesCapacity / numTotalPartitions;
  }

  @Override
  public long getCacheNumItemsCapacity() {
    if (cacheNumItemsCapacity < 0) {
      return cacheNumItemsCapacity;
    }
    return cacheNumItemsCapacity / numTotalPartitions;
  }

  @Override
  public int getBufferReuseMaxSize() {
    return bufferReuseMaxSize;
  }

  @Override
  public Set<String> getDataDirectories() {
    return dataDirectoriesConfigurator.getDataDirectories();
  }
}
