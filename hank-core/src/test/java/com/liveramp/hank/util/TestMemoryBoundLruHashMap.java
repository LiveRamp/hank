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

import com.liveramp.hank.test.BaseTestCase;

public class TestMemoryBoundLruHashMap extends BaseTestCase {

  private final MockManagedBytes k1 = new MockManagedBytes(1);
  private final MockManagedBytes v1 = new MockManagedBytes(1);

  private final MockManagedBytes k2 = new MockManagedBytes(2);
  private final MockManagedBytes v2 = new MockManagedBytes(2);

  private final MockManagedBytes k3 = new MockManagedBytes(3);
  private final MockManagedBytes v3 = new MockManagedBytes(3);

  private final MockManagedBytes k10 = new MockManagedBytes(10);
  private final MockManagedBytes v10 = new MockManagedBytes(10);

  private static class MockManagedBytes implements ManagedBytes {

    private final long numManagedBytes;

    private MockManagedBytes(long numManagedBytes) {
      this.numManagedBytes = numManagedBytes;
    }

    @Override
    public long getNumManagedBytes() {
      return numManagedBytes;
    }
  }

  public void testNumBytesCapacity() {
    MemoryBoundLruHashMap<MockManagedBytes, MockManagedBytes> map =
        new MemoryBoundLruHashMap<MockManagedBytes, MockManagedBytes>(20);

    assertEquals(0, map.size());
    assertEquals(0, map.getNumManagedBytes());

    // Insert

    map.put(k1, v1);

    assertEquals(1, map.size());
    assertEquals(2, map.getNumManagedBytes());
    assertEquals(v1, map.get(k1));

    // Insert

    map.put(k2, v2);

    assertEquals(2, map.size());
    assertEquals(6, map.getNumManagedBytes());
    assertEquals(v2, map.get(k2));

    // Insert which goes over the size threshold

    map.put(k10, v10);

    assertEquals(1, map.size());
    assertEquals(20, map.getNumManagedBytes());
    assertEquals(v10, map.get(k10));

    // Insert which goes over the size threshold

    map.put(k1, v10);

    assertEquals(1, map.size());
    assertEquals(11, map.getNumManagedBytes());
    assertEquals(v10, map.get(k1));

    // Insert that replaces a value

    map.put(k1, v1);

    assertEquals(1, map.size());
    assertEquals(2, map.getNumManagedBytes());
    assertEquals(v1, map.get(k1));

    // Insert

    map.put(k2, v1);

    assertEquals(2, map.size());
    assertEquals(5, map.getNumManagedBytes());
    assertEquals(v1, map.get(k2));
  }

  public void testNumItemsCapacity() {
    MemoryBoundLruHashMap<MockManagedBytes, MockManagedBytes> map =
        new MemoryBoundLruHashMap<MockManagedBytes, MockManagedBytes>(20, 2);

    assertEquals(0, map.size());
    assertEquals(0, map.getNumManagedBytes());

    // Insert

    map.put(k1, v1);

    assertEquals(1, map.size());
    assertEquals(2, map.getNumManagedBytes());
    assertEquals(v1, map.get(k1));

    // Insert

    map.put(k2, v2);

    assertEquals(2, map.size());
    assertEquals(6, map.getNumManagedBytes());
    assertEquals(v2, map.get(k2));

    // Insert which goes over the num items limit

    map.put(k3, v3);

    assertEquals(2, map.size());
    assertEquals(10, map.getNumManagedBytes());
    // k1 should be gone
    assertEquals(null, map.get(k1));
    assertEquals(v2, map.get(k2));
    assertEquals(v3, map.get(k3));

    // Get to change access ordering

    map.get(k2);

    // Insert which goes over the num items limit

    map.put(k1, v1);

    assertEquals(2, map.size());
    assertEquals(6, map.getNumManagedBytes());
    // k3 should be gone
    assertEquals(v1, map.get(k1));
    assertEquals(v2, map.get(k2));
    assertEquals(null, map.get(k3));
  }
}
