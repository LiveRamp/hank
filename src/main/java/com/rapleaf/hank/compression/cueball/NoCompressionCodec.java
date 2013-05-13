package com.rapleaf.hank.compression.cueball;

import com.rapleaf.hank.compression.cueball.CompressionCodec;

public class NoCompressionCodec implements CompressionCodec {
  @Override
  public int compress(byte[] src, int srcOffset, int srcLength, byte[] dst, int dstOff) {
    System.arraycopy(src, srcOffset, dst, dstOff, srcLength);
    return srcLength - srcOffset;
  }

  @Override
  public int decompress(byte[] src, int srcOffset, int srcLength, byte[] dst, int dstOff) {
    System.arraycopy(src, srcOffset, dst, dstOff, srcLength);
    return srcLength - srcOffset;
  }

  @Override
  public int getMaxCompressBufferSize(int length) {
    return length;
  }
}
