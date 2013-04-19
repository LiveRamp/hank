package com.rapleaf.hank.compress;

public interface CompressionCodec {

  public int compress(byte[] src, int srcOffset, int srcLength, byte[] dst, int dstOff);

  public int getMaxCompressBufferSize(int length);

  public int decompress(byte[] src, int srcOffset, int srcLength, byte[] dst, int dstOff);
}
