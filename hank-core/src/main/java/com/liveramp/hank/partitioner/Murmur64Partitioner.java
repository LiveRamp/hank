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
package com.liveramp.hank.partitioner;

import com.liveramp.commons.util.BytesUtils;
import com.liveramp.hank.hasher.Murmur64Hasher;

import java.nio.ByteBuffer;

/**
 * Implementation of Partitioner that takes a 64-bit Murmur hash to produce the
 * partition number.
 */
public class Murmur64Partitioner implements Partitioner {
  /**
   * Note that we use a wacky, unique seed here in order to make sure that no
   * one else will accidentally use a matching MurmurHash somewhere that can
   * screw us up. (Like in the Hasher for Curly, for instance.)
   */
  private static final int SEED = 645568;

  @Override
  public int partition(ByteBuffer key, int numPartitions) {
    return Math.abs((int) Murmur64Hasher.murmurHash64(key.array(),
        key.arrayOffset() + key.position(),
        key.remaining(),
        SEED)) % numPartitions;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof Murmur64Partitioner;
  }

  @Override
  public String toString() {
    return Murmur64Partitioner.class.getSimpleName();
  }

  public static void main(String[] args) {
    System.out.println(new Murmur64Partitioner().partition(BytesUtils.hexStringToBytes(args[0]), Integer.valueOf(args[1])));
  }
}
