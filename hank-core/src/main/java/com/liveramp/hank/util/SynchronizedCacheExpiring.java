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

public class SynchronizedCacheExpiring<K, V> {

  private final LruHashMap<K, ValueAndTimestamp<V>> cache;
  private final long expirationPeriodMs;

  // A cache capacity <= 0 will disable the cache and will not add any synchronization overhead
  public SynchronizedCacheExpiring(int cacheCapacity, long expirationPeriodSeconds) {
    if (cacheCapacity <= 0) {
      cache = null;
    } else {
      cache = new LruHashMap<K, ValueAndTimestamp<V>>(0, cacheCapacity);
    }
    this.expirationPeriodMs = expirationPeriodSeconds * 1000;
  }

  public boolean isActive() {
    return cache != null;
  }

  public V get(K key) {
    if (cache == null) {
      return null;
    } else {
      ValueAndTimestamp<V> cachedValue;
      synchronized (cache) {
        // Attempt to get from cache
        cachedValue = cache.get(key);
        // Expire if needed
        if (cachedValue != null && shouldExpire(cachedValue)) {
          cache.remove(key);
          cachedValue = null;
        }
      }
      if (cachedValue == null) {
        return null;
      } else {
        return cachedValue.getValue();
      }
    }
  }

  public void put(K key, V value) {
    if (cache != null) {
      if (value == null) {
        throw new IllegalArgumentException("Value to put in cache should not be null.");
      }
      synchronized (cache) {
        cache.put(key, new ValueAndTimestamp<V>(value, System.currentTimeMillis()));
      }
    }
  }

  protected boolean shouldExpire(ValueAndTimestamp<V> valueAndTimestamp) {
    return (System.currentTimeMillis() - valueAndTimestamp.getTimestamp()) >= expirationPeriodMs;
  }

  private static class ValueAndTimestamp<V> {

    private final V value;
    private final long timestamp;

    public ValueAndTimestamp(V value, long timestamp) {
      this.value = value;
      this.timestamp = timestamp;
    }

    public V getValue() {
      return value;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }
}
