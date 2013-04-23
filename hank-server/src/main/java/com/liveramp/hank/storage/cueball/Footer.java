package com.liveramp.hank.storage.cueball;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.liveramp.hank.util.EncodingHelper;

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
      final long offset = EncodingHelper.decodeLittleEndianFixedWidthLong(footer, i * 8, 8);
      if (offset < -1) {
        throw new IOException(String.format("Read an unexpectedly negative block offset (%d) at block position %d!", offset, i));
      }
      if (offset != -1 && i > 0 && hashIndex[i-1] != -1 && hashIndex[i-1] >= offset) {
        throw new IOException(String.format("Discovered an offset inversion! block %d offset: %d, block %d offset: %d", i-1, hashIndex[i-1], i, offset));
      }
      getHashIndex()[i] = offset;
    }

    maxUncompressedBufferSize = (int) EncodingHelper.decodeLittleEndianFixedWidthLong(footer, footer.length - 8, 4);
    if (maxUncompressedBufferSize < 0) {
      throw new IOException(String.format("Read an invalid max uncompressed buffer size of %d!", maxUncompressedBufferSize));
    }
    maxCompressedBufferSize = (int) EncodingHelper.decodeLittleEndianFixedWidthLong(footer, footer.length - 4, 4);
    if (maxCompressedBufferSize < 0) {
      throw new IOException(String.format("Read an invalid max uncompressed buffer size of %d!", maxCompressedBufferSize));
    }
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
