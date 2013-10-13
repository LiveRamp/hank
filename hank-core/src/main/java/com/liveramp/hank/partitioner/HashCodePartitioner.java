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

package com.liveramp.hank.partitioner;

import java.nio.ByteBuffer;

public class HashCodePartitioner implements Partitioner {

  @Override
  public int partition(ByteBuffer key, int numPartitions) {
    int hashCode = 31 + (key != null ? bytesHashCode(key.array(), key.arrayOffset() + key.position(), key.remaining()) : 0);
    return (hashCode & Integer.MAX_VALUE) % numPartitions;
  }

  private int bytesHashCode(byte[] bytes, int offset, int length) {
    int hash = 1;
    for (int i = offset; i < offset + length; i++) {
      hash = (31 * hash) + (int)bytes[i];
    }
    return hash;
  }
}
