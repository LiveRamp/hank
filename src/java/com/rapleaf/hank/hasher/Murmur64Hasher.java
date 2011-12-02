/**
 *  Copyright 2011 Rapleaf
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
package com.rapleaf.hank.hasher;

import java.nio.ByteBuffer;

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup. See http://murmurhash.googlepages.com/ for more details.
 */
public final class Murmur64Hasher implements Hasher {
  /**
   * Note that we use a random initial seed here so that we are unlikely to get
   * the "same" MurmurHash as anyone else. (For a while, we were mistakenly
   * using the same exact hash in the Partitioner as the Hasher, leading to very
   * strange behavior.)
   */
  private static final int INITIAL_SEED = 952336;

  public static long murmurHash64(final byte[] data, final int off, final int length, final int seed) {
    final long m = 0xc6a4a7935bd1e995L;
    final int r = 47;

    long h = seed ^ (length * m);

    final int remainder = length & 7;
    final int end = length - remainder;
    for (int i = off; i < end; i += 8) {
      long k = data[i + 7];
      k = k << 8;
      k = k | (data[i + 6] & 0xff);
      k = k << 8;
      k = k | (data[i + 5] & 0xff);
      k = k << 8;
      k = k | (data[i + 4] & 0xff);
      k = k << 8;
      k = k | (data[i + 3] & 0xff);
      k = k << 8;
      k = k | (data[i + 2] & 0xff);
      k = k << 8;
      k = k | (data[i + 1] & 0xff);
      k = k << 8;
      k = k | (data[i + 0] & 0xff);

      k *= m;
      k ^= k >>> r;
      k *= m;

      h ^= k;
      h *= m;
    }

    switch (remainder) {
      case 7:
        h ^= (long) (data[end + 6] & 0xff) << 48;
      case 6:
        h ^= (long) (data[end + 5] & 0xff) << 40;
      case 5:
        h ^= (long) (data[end + 4] & 0xff) << 32;
      case 4:
        h ^= (long) (data[end + 3] & 0xff) << 24;
      case 3:
        h ^= (long) (data[end + 2] & 0xff) << 16;
      case 2:
        h ^= (long) (data[end + 1] & 0xff) << 8;
      case 1:
        h ^= (long) (data[end] & 0xff);
        h *= m;
    }

    h ^= h >>> r;
    h *= m;
    h ^= h >>> r;

    return h;
  }

  @Override
  public void hash(ByteBuffer value, int hashSize, byte[] hashBytes) {
    int seed = INITIAL_SEED;
    long hashValue = 0;
    for (int i = 0; i < hashSize - 8; i += 8) {
      hashValue = murmurHash64(value.array(), value.arrayOffset() + value.position(), value.arrayOffset() + value.limit(), seed);
      seed = (int) hashValue;
      hashBytes[i] = (byte) ((hashValue >> 56) & 0xff);
      hashBytes[i + 1] = (byte) ((hashValue >> 48) & 0xff);
      hashBytes[i + 2] = (byte) ((hashValue >> 40) & 0xff);
      hashBytes[i + 3] = (byte) ((hashValue >> 32) & 0xff);
      hashBytes[i + 4] = (byte) ((hashValue >> 24) & 0xff);
      hashBytes[i + 5] = (byte) ((hashValue >> 16) & 0xff);
      hashBytes[i + 6] = (byte) ((hashValue >> 8) & 0xff);
      hashBytes[i + 7] = (byte) ((hashValue) & 0xff);
    }

    int shortHashBytes = hashSize % 8;
    if (shortHashBytes > 0) {
      hashValue = murmurHash64(value.array(), value.arrayOffset() + value.position(), value.arrayOffset() + value.limit(), seed);
      int off = hashSize - 1;
      switch (shortHashBytes) {
        case 7:
          hashBytes[off--] = (byte) ((hashValue >> 8) & 0xff);
        case 6:
          hashBytes[off--] = (byte) ((hashValue >> 16) & 0xff);
        case 5:
          hashBytes[off--] = (byte) ((hashValue >> 24) & 0xff);
        case 4:
          hashBytes[off--] = (byte) ((hashValue >> 32) & 0xff);
        case 3:
          hashBytes[off--] = (byte) ((hashValue >> 40) & 0xff);
        case 2:
          hashBytes[off--] = (byte) ((hashValue >> 48) & 0xff);
        case 1:
          hashBytes[off--] = (byte) ((hashValue >> 56) & 0xff);
      }
    }
  }

  @Override
  public String toString() {
    return "Murmur64Hasher";
  }
}
