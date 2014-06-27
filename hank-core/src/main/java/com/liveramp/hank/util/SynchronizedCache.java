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

import com.liveramp.commons.collections.LruHashMap;

public class SynchronizedCache<K, V> {

  private final LruHashMap<K, V> cache;

  // A disabled cache will not add any synchronization overhead
  public SynchronizedCache(boolean isEnabled, int cacheCapacity) {
    if (isEnabled) {
      cache = new LruHashMap<K, V>(cacheCapacity, 0);
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
      V cachedValue;
      synchronized (cache) {
        cachedValue = cache.get(key);
      }
      if (cachedValue != null) {
        return cachedValue;
      } else {
        return null;
      }
    }
  }

  public void put(K key, V value) {
    if (isEnabled()) {
      if (value == null) {
        throw new IllegalArgumentException("Value to put in cache should not be null.");
      }
      synchronized (cache) {
        cache.put(key, value);
      }
    }
  }
}
