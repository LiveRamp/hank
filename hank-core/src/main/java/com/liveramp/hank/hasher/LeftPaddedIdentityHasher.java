package com.liveramp.hank.hasher;

import java.nio.ByteBuffer;

public class LeftPaddedIdentityHasher implements Hasher {
  @Override
  public void hash(ByteBuffer value, int hashSize, byte[] hashBytes) {

    if (value.remaining() > hashSize) {
      throw new IllegalArgumentException("Cannot pad incoming item "+value+" to length "+hashSize+"!");
    }

    int bytesToPad = hashSize - value.remaining();

    System.arraycopy(value.array(), value.arrayOffset() + value.position(), hashBytes, bytesToPad, value.remaining());

  }
}
