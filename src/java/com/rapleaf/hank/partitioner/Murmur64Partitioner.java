package com.rapleaf.hank.partitioner;

import java.nio.ByteBuffer;

import com.rapleaf.hank.hasher.Murmur64Hasher;

public class Murmur64Partitioner implements Partitioner {
  @Override
  public int partition(ByteBuffer key) {
    return Math.abs((int) Murmur64Hasher.murmurHash64(key.array(), key.arrayOffset() + key.position(), key.remaining(), 1));
  }
}
