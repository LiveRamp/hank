package com.rapleaf.hank.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class JavaGzipCompressionCodec implements CompressionCodec {
  @Override
  public int compress(byte[] src, int srcOffset, int srcLength, byte[] dst, int dstOff) {
    if (srcLength - srcOffset == 0) {
      return 0;
    }
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream(srcLength - srcOffset);
      GZIPOutputStream gzip = new GZIPOutputStream(bytesOut);
      gzip.write(src, srcOffset, srcLength);
      gzip.flush();
      gzip.close();
      byte[] compressed = bytesOut.toByteArray();
      System.arraycopy(compressed, 0, dst, dstOff, compressed.length);
      return compressed.length;
    } catch (IOException e) {
      throw new RuntimeException("Unexpected IOException while compressing!", e);
    }
  }

  @Override
  public int decompress(byte[] src, int srcOffset, int srcLength, byte[] dst, int dstOff) {
    if (srcLength - srcOffset == 0) {
      return 0;
    }
    try {
      ByteArrayInputStream bytesIn = new ByteArrayInputStream(src, srcOffset, srcLength);
      GZIPInputStream gzip = new GZIPInputStream(bytesIn);
      int curOff = dstOff;
      while (true) {
        int amtRead = gzip.read(dst, curOff, dst.length - curOff);
        if (amtRead == -1) {
          break;
        }
        curOff += amtRead;
      }
      return curOff;
    } catch (IOException e) {
      throw new RuntimeException("Unexpected IOException while decompressing!", e);
    }
  }

  @Override
  public int getMaxCompressBufferSize(int length) {
    // NOTE: this is a total guess. Let's hope your data isn't so crappy that it
    // doubles in size when compressed with gzip.
    return (int) (1.5 * length) + 18;
  }
}
