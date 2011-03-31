package com.rapleaf.hank.storage.cueball;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.rapleaf.hank.util.EncodingHelper;

final class Footer {
  private final long[] hashIndex;
  private final int maxUncompressedBufferSize;
  private final int maxCompressedBufferSize;
  private final long fileSize;
  private final int footerLength;

  public Footer(FileChannel channel, int hashIndexBits) throws IOException {
    fileSize = channel.size();

    final int hashIndexSize = 1 << hashIndexBits;
    footerLength = hashIndexSize * 8 + 8;

    byte[] footer = new byte[getFooterLength()];
    int read = channel.read(ByteBuffer.wrap(footer), getFileSize() - getFooterLength());
    if (read != getFooterLength()) {
      throw new IOException("Tried to read " + getFooterLength() + " bytes of footer, but only got " + read + " bytes!");
    }

    hashIndex = new long[hashIndexSize];
    for (int i = 0; i < getHashIndex().length; i++) {
      getHashIndex()[i] = EncodingHelper.decodeLittleEndianFixedWidthLong(footer, i * 8, 8);
    }

    maxUncompressedBufferSize = (int) EncodingHelper.decodeLittleEndianFixedWidthLong(footer, footer.length - 8, 4);
    maxCompressedBufferSize = (int) EncodingHelper.decodeLittleEndianFixedWidthLong(footer, footer.length - 4, 4);
  }

  public long[] getHashIndex() {
    return hashIndex;
  }

  public int getMaxCompressedBufferSize() {
    return maxCompressedBufferSize;
  }

  public int getMaxUncompressedBufferSize() {
    return maxUncompressedBufferSize;
  }

  public long getFileSize() {
    return fileSize;
  }

  public int getFooterLength() {
    return footerLength;
  }
  
  public long getDataLength() {
    return getFileSize() - getFooterLength();
  }
}
