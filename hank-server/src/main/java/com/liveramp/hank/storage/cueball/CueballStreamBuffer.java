/**
 *  Copyright 2011 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
/**
 *
 */
package com.liveramp.hank.storage.cueball;

import com.liveramp.commons.util.BytesUtils;
import com.liveramp.hank.compression.cueball.CueballCompressionCodec;
import com.liveramp.hank.util.IOStreamUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public final class CueballStreamBuffer {
  private final int relativeIndex;
  private final int keyHashSize;
  private int currentOffset = 0;
  private int currentLimit = 0;
  private final int fullRecordSize;

  private boolean complete;
  private final InputStream stream;

  private final long[] hashIndex;
  private final byte[] uncompressedBuffer;
  private final byte[] compressedBuffer;

  private int currentHashIndexIdx = -1;
  private final CueballCompressionCodec compressionCodec;
  private final long dataLength;

  public CueballStreamBuffer(String filePath,
                             int relativeIndex,
                             int keyHashSize,
                             int valueSize,
                             int hashIndexBits,
                             CueballCompressionCodec compressionCodec)
      throws IOException {
    this.relativeIndex = relativeIndex;
    this.compressionCodec = compressionCodec;
    FileInputStream fileInputStream = new FileInputStream(filePath);
    this.stream = new BufferedInputStream(fileInputStream, IOStreamUtils.DEFAULT_BUFFER_SIZE);

    this.keyHashSize = keyHashSize;
    this.fullRecordSize = valueSize + keyHashSize;

    Footer footer = new Footer(fileInputStream.getChannel(), hashIndexBits);
    dataLength = footer.getDataLength();
    hashIndex = footer.getHashIndex();
    uncompressedBuffer = new byte[footer.getMaxUncompressedBufferSize()];
    compressedBuffer = new byte[footer.getMaxCompressedBufferSize()];
  }

  public boolean anyRemaining() throws IOException {
    if (currentOffset < currentLimit) {
      return true;
    }
    if (complete) {
      return false;
    }

    // refill the buffer

    // advance to the next non-empty block
    currentHashIndexIdx++;
    while (currentHashIndexIdx < hashIndex.length) {
      if (hashIndex[currentHashIndexIdx] != -1) {
        break;
      }
      currentHashIndexIdx++;
    }

    if (currentHashIndexIdx >= hashIndex.length) {
      // there are no more blocks. we're all done
      complete = true;
      return false;
    }

    // there's at least one block left.

    long upperOffset;
    if (currentHashIndexIdx == hashIndex.length - 1) {
      // actually, there's *exactly* one block left. we need to compare the
      // current offset to the data length to determine the compressed block
      // size.
      upperOffset = dataLength;
    } else {
      // so this isn't the last block in the index, but it might still be the
      // last block in the file.
      int nextHashIndexIdx = currentHashIndexIdx + 1;
      while (nextHashIndexIdx < hashIndex.length) {
        if (hashIndex[nextHashIndexIdx] != -1) {
          break;
        }
        nextHashIndexIdx++;
      }
      // need to examine the "next" we got back...
      if (nextHashIndexIdx == hashIndex.length) {
        // turns out that it was the last block in the file, so we should
        // compare against the end of the data length.
        upperOffset = dataLength;
      } else {
        // sweet, there's another block there somewhere. use that offset as the
        // one to determine our compressed block length.
        upperOffset = hashIndex[nextHashIndexIdx];
      }
    }

    final int blockLength = (int) (upperOffset - hashIndex[currentHashIndexIdx]);
    // read the compressed block from disk into the compressed buffer
    final int compressedBytesRead = stream.read(compressedBuffer, 0, blockLength);
    // decompress the compressed block into the uncompressed buffer
    final int decompressedSize = compressionCodec.decompress(compressedBuffer, 0, compressedBytesRead, uncompressedBuffer, 0);

    // adjust the pointers
    currentOffset = 0;
    currentLimit = decompressedSize;
    return true;
  }

  /**
   * Compare the key hash at the current position of two stream buffers
   *
   * @param other The stream buffer to compare this to
   * @return
   */
  public int compareTo(CueballStreamBuffer other) {
    return BytesUtils.compareBytesUnsigned(uncompressedBuffer,
        currentOffset,
        other.uncompressedBuffer,
        other.getCurrentOffset(),
        keyHashSize);
  }

  public void consume() {
    currentOffset += fullRecordSize;
  }

  public int getIndex() {
    return relativeIndex;
  }

  public byte[] getBuffer() {
    return uncompressedBuffer;
  }

  public int getCurrentOffset() {
    return currentOffset;
  }

  public void close() throws IOException {
    stream.close();
  }
}
