/**
 *  Copyright 2011 LiveRamp
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
package com.liveramp.hank.test.partitioner;

import com.liveramp.commons.util.BytesUtils;
import com.liveramp.hank.partitioner.Partitioner;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class MapPartitioner implements Partitioner {
  private final Map<ByteBuffer, Integer> map = new HashMap<ByteBuffer, Integer>();

  public MapPartitioner(Object... objects) {
    for (int i = 0; i < objects.length; i += 2) {
      map.put((ByteBuffer) objects[i], (Integer) objects[i + 1]);
    }
  }

  @Override
  public int partition(ByteBuffer key, int numPartitions) {
    if (key == null) {
      throw new RuntimeException("Given key is null.");
    }
    Integer partition = map.get(key);
    if (partition == null) {
      throw new RuntimeException("Key not found in the supplied map: " + BytesUtils.bytesToHexString(key));
    }
    if (!(partition < numPartitions)) {
      throw new RuntimeException("Partition " + map.get(key)
          + " falls out of the range. Number of partitions: " + numPartitions);
    }
    return partition;
  }
}
