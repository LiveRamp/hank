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

import java.util.Iterator;
import java.util.Map;

public class MemoryBoundLruHashMap<K extends ManagedBytes, V extends ManagedBytes> {

  private long numManagedBytes = 0;
  private final long maxNumManagedBytes;
  private final LruHashMap<K, V> map;

  // Negative capacity values disable the corresponding check
  public MemoryBoundLruHashMap(long maxNumManagedBytes) {
    this(maxNumManagedBytes, -1);
  }

  // Negative capacity values disable the corresponding check
  public MemoryBoundLruHashMap(long maxNumManagedBytes, int maxNumItems) {
    this.maxNumManagedBytes = maxNumManagedBytes;
    map = new LruHashMap<K, V>(0, maxNumItems);
  }

  public void put(K key, V value) {
    // First, remove from map if it exists
    if (map.containsKey(key)) {
      V oldValue = map.remove(key);
      unmanage(key, oldValue);
    }

    // Add to map
    map.put(key, value);
    manage(key, value);

    // If an eldest element was removed, update byte count
    Map.Entry<K, V> eldestRemoved = map.getAndClearEldestRemoved();
    if (eldestRemoved != null) {
      unmanage(eldestRemoved);
    }

    // Now remove elements until byte count is under the threshold
    if (maxNumManagedBytes >= 0) {
      while (numManagedBytes > maxNumManagedBytes && map.size() > 0) {
        Iterator<Map.Entry<K, V>> iterator = map.entrySet().iterator();
        Map.Entry<K, V> eldest = iterator.next();
        unmanage(eldest);
        iterator.remove();
      }
    }
  }

  public V get(K key) {
    return map.get(key);
  }

  public V remove(K key) {
    if (map.containsKey(key)) {
      V value = map.remove(key);
      unmanage(key, value);
      return value;
    } else {
      return null;
    }
  }

  public int size() {
    return map.size();
  }

  public long getNumManagedBytes() {
    return numManagedBytes;
  }

  private void manage(K key, V value) {
    numManagedBytes += key.getNumManagedBytes() + value.getNumManagedBytes();
  }

  private void unmanage(K key, V value) {
    numManagedBytes -= key.getNumManagedBytes() + value.getNumManagedBytes();
  }

  private void unmanage(Map.Entry<K, V> entry) {
    numManagedBytes -= entry.getKey().getNumManagedBytes() + entry.getValue().getNumManagedBytes();
  }

  public long getMaxNumManagedBytes() {
    return maxNumManagedBytes;
  }

  public int getMaxNumItems() {
    return map.getMaxCapacity();
  }
}
