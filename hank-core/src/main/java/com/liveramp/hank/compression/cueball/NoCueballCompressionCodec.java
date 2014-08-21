package com.liveramp.hank.compression.cueball;

public class NoCueballCompressionCodec implements CueballCompressionCodec {
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
