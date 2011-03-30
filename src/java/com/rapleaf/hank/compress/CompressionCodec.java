package com.rapleaf.hank.compress;

public interface CompressionCodec {
  /**
   * Compress <i>src</i> into <i>dst</i>.
   * @param src
   * @param srcOffset
   * @param srcLength
   * @param dst
   * @param dstOff
   * @return The number of compressed bytes written to dst.
   */
  public int compress(byte[] src, int srcOffset, int srcLength, byte[] dst, int dstOff);

  /**
   * Returns the maximum number of bytes that will be required to compress
   * <i>length</i> bytes. Note that the actual compressed size will often be
   * much less.
   * 
   * @param length
   * @return
   */
  public int getMaxCompressBufferSize(int length);

  /**
   * Decompress <i>src</i> into <i>dst</i>.
   * @param src
   * @param srcOffset
   * @param srcLength
   * @param dst
   * @param dstOff
   * @return
   */
  public int decompress(byte[] src, int srcOffset, int srcLength, byte[] dst, int dstOff);
}
