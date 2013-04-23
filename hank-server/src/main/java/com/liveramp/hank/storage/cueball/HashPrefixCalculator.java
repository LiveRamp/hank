package com.liveramp.hank.storage.cueball;

public final class HashPrefixCalculator {
  private final int numBits;

  public HashPrefixCalculator(int numBits) {
    this.numBits = numBits;
  }

  public final int getHashPrefix(final byte[] chunkBytes, int off) {
    final int bitsFromLastByte = numBits % 8;
    final int numFullBytes = off + (numBits / 8);

    int prefix = 0;
    for (; off < numFullBytes; off++) {
      prefix = (prefix << 8) | (chunkBytes[off] & 0xff);
    }
    if (bitsFromLastByte == 0) {
      return prefix;
    } else {
      return (prefix << bitsFromLastByte) | (((chunkBytes[off] & 0xff) >> (8 - bitsFromLastByte)));
    }
  }
}
