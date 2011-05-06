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
package com.rapleaf.hank.storage.cueball;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.rapleaf.hank.compress.CompressionCodec;
import com.rapleaf.hank.hasher.Hasher;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.util.EncodingHelper;

/**
 * Note that the current implementation does not support writing partitions with
 * more than 20000 entries per block.
 */
public class CueballWriter implements Writer {
  private static final int DEFAULT_NUMBER_OF_ENTRIES = 20000;
  private final OutputStream stream;
  private final int keyHashSize;
  private final Hasher hasher;
  private final int valueSize;
  private final CompressionCodec compressionCodec;

  private final byte[] uncompressedBuffer;
  private final byte[] compressedBuffer;
  private final byte[] keyHashBytes;

  private final long[] hashIndex;

  private final HashPrefixCalculator prefixer;
  private int lastHashPrefix = -1;
  private int uncompressedOffset = 0;

  private int bytesWritten = 0;
  private long maxUncompressedBlockSize;
  private long maxCompressedBlockSize;

  public CueballWriter(OutputStream outputStream,
      int keyHashSize,
      Hasher hasher,
      int valueSize,
      CompressionCodec compressionCodec,
      int hashIndexBits)
  {
    this.stream = outputStream;
    this.keyHashSize = keyHashSize;
    this.hasher = hasher;
    this.valueSize = valueSize;
    this.compressionCodec = compressionCodec;

    uncompressedBuffer = new byte[(keyHashSize + valueSize) * DEFAULT_NUMBER_OF_ENTRIES];
    compressedBuffer = new byte[compressionCodec.getMaxCompressBufferSize(uncompressedBuffer.length)];
    keyHashBytes = new byte[keyHashSize];

    prefixer = new HashPrefixCalculator(hashIndexBits);

    hashIndex = new long[1 << hashIndexBits];
    Arrays.fill(hashIndex, -1);
  }

  @Override
  public void write(ByteBuffer key, ByteBuffer value) throws IOException {
    hasher.hash(key, keyHashBytes);
    writeHash(ByteBuffer.wrap(keyHashBytes), value);
  }

  public void writeHash(ByteBuffer hashedKey, ByteBuffer value) throws IOException {
    // check the first hashIndexBits of the hashedKey
    int thisPrefix = prefixer.getHashPrefix(hashedKey.array(), hashedKey.arrayOffset() + hashedKey.position());

    // if this prefix and the last one don't match, then it's time to clear the
    // buffer.
    if (lastHashPrefix == -1 || thisPrefix != lastHashPrefix) {
      if (thisPrefix < lastHashPrefix) {
        throw new IOException("Just found a hash prefix inversion!");
      }
      // clear the uncompressed buffer
      clearUncompressed();

      // start over in the buffer
      uncompressedOffset = 0;
      lastHashPrefix = thisPrefix;

      // record the start index of the next block
      hashIndex[thisPrefix] = bytesWritten;
    }

    // at this point, we're guaranteed to be ready to write to the buffer.

    // TODO: remove, this should be checked elsewhere
    if (hashedKey.array().length - hashedKey.arrayOffset() + hashedKey.position() < keyHashSize ||
        uncompressedBuffer.length - uncompressedOffset < keyHashSize) {
      throw new RuntimeException("Could not copy hashedKey length: " + hashedKey.array().length + " offset: " + hashedKey.arrayOffset() + hashedKey.position() +
          ", into uncompressed buffer length: " + uncompressedBuffer.length + " offset: " + uncompressedOffset + ", keyHashSize: " + keyHashSize);
    }

    // write a subsequence of the key hash's bytes
    System.arraycopy(hashedKey.array(), hashedKey.arrayOffset() + hashedKey.position(), uncompressedBuffer, uncompressedOffset, keyHashSize);

    // encode the value offset and write it out
    System.arraycopy(value.array(), value.arrayOffset() + value.position(), uncompressedBuffer, uncompressedOffset + keyHashSize, valueSize);
    uncompressedOffset += keyHashSize + valueSize;
  }

  private void clearUncompressed() throws IOException {
    // compress the block
    int compressedSize = compressionCodec.compress(uncompressedBuffer, 0, uncompressedOffset, compressedBuffer, 0);
    // write the compressed block to the data stream
    stream.write(compressedBuffer, 0, compressedSize);
    bytesWritten += compressedSize;

    // keep track of the max block sizes
    if (uncompressedOffset > maxUncompressedBlockSize) {
      maxUncompressedBlockSize = uncompressedOffset;
    }

    if (compressedSize > maxCompressedBlockSize) {
      maxCompressedBlockSize = compressedSize;
    }
  }

  @Override
  public void close() throws IOException {
    // clear the last block, if there is one
    if (uncompressedOffset > 0) {
      clearUncompressed();
    }

    // serialize the footer
    byte[] footer = new byte[8 * hashIndex.length + 4 + 4];

    for (int i = 0; i < hashIndex.length; i++) {
      EncodingHelper.encodeLittleEndianFixedWidthLong(hashIndex[i], footer, i * 8, 8);
    }

    // write the buffer size hints
    EncodingHelper.encodeLittleEndianFixedWidthLong(maxUncompressedBlockSize, footer, footer.length - 8, 4);
    EncodingHelper.encodeLittleEndianFixedWidthLong(maxCompressedBlockSize, footer, footer.length - 4, 4);

    stream.write(footer);

    // flush everything and close
    stream.flush();
    stream.close();
  }
}
