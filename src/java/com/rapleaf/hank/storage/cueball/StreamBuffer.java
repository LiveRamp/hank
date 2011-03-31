/**
 *  Copyright 2011 Rapleaf
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
package com.rapleaf.hank.storage.cueball;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.rapleaf.hank.compress.CompressionCodec;
import com.rapleaf.hank.util.Bytes;

public final class StreamBuffer {
  private final int relativeIndex;
  private final int keyHashSize;
  private int currentOffset = 0;
  private int currentLimit = 0;
  private final int fullRecordSize;

  private boolean complete;
  private final FileChannel channel;

  private final long[] hashIndex;
  private final byte[] uncompressedBuffer;
  private final byte[] compressedBuffer;

  private int currentHashIndexIdx = -1;
  private final CompressionCodec compressionCodec;
  private final long dataLength;

  public StreamBuffer(String filePath,
      int relativeIndex,
      int keyHashSize,
      int valueSize,
      int hashIndexBits,
      CompressionCodec compressionCodec)
  throws IOException {
    this.relativeIndex = relativeIndex;
    this.compressionCodec = compressionCodec;
    this.channel = new FileInputStream(filePath).getChannel();

    this.keyHashSize = keyHashSize;
    this.fullRecordSize = valueSize + keyHashSize;

    Footer footer = new Footer(channel, hashIndexBits);
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

    // advance to the next block
    currentHashIndexIdx++;

    if (currentHashIndexIdx >= hashIndex.length) {
      // we've already processed the last block in the file.
      return false;
    }

    // special case: if we're on the last block in the file, then we need to
    // compare the current offset to the data length to determine the compressed
    // block size.
    long upperOffset;
    if (currentHashIndexIdx == hashIndex.length - 1) {
      upperOffset = dataLength;
    } else {
      upperOffset = hashIndex[currentHashIndexIdx+1];
    }
    final int blockLength = (int) (upperOffset - hashIndex[currentHashIndexIdx]);
    // read the compressed block from disk into the compressed buffer
    final int compressedBytesRead = readFully(channel, compressedBuffer, blockLength);
    // decompress the compressed block into the uncompressed buffer
    final int decompressedSize = compressionCodec.decompress(compressedBuffer, 0, compressedBytesRead, uncompressedBuffer, 0);

    // adjust the pointers 
    currentOffset = 0;
    currentLimit = decompressedSize;
    return true;
  }

  public int compareTo(StreamBuffer other) {
    return Bytes.compareBytes(uncompressedBuffer,
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

  private static int readFully(FileChannel channel, byte[] buf, int readLength) throws IOException {
    return channel.read(ByteBuffer.wrap(buf, 0, readLength));
  }

  public void close() throws IOException {
    channel.close();
  }
}