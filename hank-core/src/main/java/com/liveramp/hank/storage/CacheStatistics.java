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

package com.liveramp.hank.storage;

public class CacheStatistics {

  private long numItems;
  private long maxNumItems;
  private long numManagedBytes;
  private long maxNumManagedBytes;

  public CacheStatistics(long numItems, long maxNumItems, long numManagedBytes, long maxNumManagedBytes) {
    this.numItems = numItems;
    this.maxNumItems = maxNumItems;
    this.numManagedBytes = numManagedBytes;
    this.maxNumManagedBytes = maxNumManagedBytes;
  }

  public long getNumItems() {
    return numItems;
  }

  public long getMaxNumItems() {
    return maxNumItems;
  }

  public long getNumManagedBytes() {
    return numManagedBytes;
  }

  public long getMaxNumManagedBytes() {
    return maxNumManagedBytes;
  }

  public void add(CacheStatistics cacheStatistics) {
    this.numItems += cacheStatistics.numItems;
    this.maxNumItems += cacheStatistics.maxNumItems;
    this.numManagedBytes += cacheStatistics.numManagedBytes;
    this.maxNumManagedBytes += cacheStatistics.maxNumManagedBytes;
    if (this.maxNumItems < 0) {
      this.maxNumItems = -1;
    }
    if (this.maxNumManagedBytes < 0) {
      this.maxNumManagedBytes = -1;
    }
  }
}
