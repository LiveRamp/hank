package com.rapleaf.hank.compress;

public class NoCompressionCodec implements CompressionCodec {
  @Override
  public int compress(byte[] src, int srcOffset, int srcLength, byte[] dst, int dstOff) {
    return srcLength - srcOffset;
  }

  @Override
  public int decompress(byte[] src, int srcOffset, int srcLength, byte[] dst, int dstOff) {
    return srcLength - srcOffset;
  }

  @Override
  public int getMaxCompressBufferSize(int length) {
    return length;
  }
}
