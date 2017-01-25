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

package com.liveramp.hank.util;

import com.liveramp.commons.collections.MemoryBoundLruHashMap;
import com.liveramp.commons.util.MemoryUsageEstimator;

public class SynchronizedMemoryBoundCache<K, V> {

  private final com.liveramp.commons.collections.MemoryBoundLruHashMap<K, V> cache;

  // A disabled cache will not add any synchronization overhead
  public SynchronizedMemoryBoundCache(boolean isEnabled,
                                      long numBytesCapacity,
                                      int numItemsCapacity,
                                      MemoryUsageEstimator<K> keyEstimator,
                                      MemoryUsageEstimator<V> valueEstimator) {
    if (isEnabled) {
      cache = new MemoryBoundLruHashMap<K, V>(numItemsCapacity, numBytesCapacity, keyEstimator, valueEstimator);
    } else {
      cache = null;
    }
  }

  public boolean isEnabled() {
    return cache != null;
  }

  public V get(K key) {
    if (!isEnabled()) {
      return null;
    } else {
      synchronized (cache) {
        return cache.get(key);
      }
    }
  }

  public void put(K key, V value) {
    if (isEnabled()) {
      if (value == null) {
        throw new IllegalArgumentException("Value to put in cache should not be null.");
      }
      synchronized (cache) {
        cache.putAndEvict(key, value);
      }
    }
  }

  public int size() {
    if (!isEnabled()) {
      return 0;
    } else {
      synchronized (cache) {
        return cache.size();
      }
    }
  }

  public int getMaxNumItems() {
    if(!isEnabled()){
      return 0;
    }else{
      return cache.getMaxNumItems();
    }
  }

  public long getNumManagedBytes() {
    if (!isEnabled()) {
      return 0;
    } else {
      synchronized (cache) {
        return cache.getNumManagedBytes();
      }
    }
  }

  public long getMaxNumManagedBytes() {
    if(!isEnabled()){
      return 0;
    }else{
      return cache.getMaxNumManagedBytes();
    }
  }
}
