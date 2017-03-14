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
package com.liveramp.hank.storage.cueball;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.liveramp.commons.util.BytesUtils;
import com.liveramp.hank.compression.cueball.CueballCompressionCodec;
import com.liveramp.hank.hasher.Hasher;
import com.liveramp.hank.storage.Writer;
import com.liveramp.hank.util.EncodingHelper;
import com.liveramp.hank.util.IOStreamUtils;

/**
 * Note that the current implementation does not support writing partitions with
 * more than 80000 entries per block.
 */
public class CueballWriter implements Writer {
  private static final int DEFAULT_NUMBER_OF_ENTRIES = 80000;
  private final OutputStream stream;
  private final int keyHashSize;
  private final Hasher hasher;
  private final int valueSize;
  private final CueballCompressionCodec compressionCodec;

  private final byte[] uncompressedBuffer;
  private final byte[] compressedBuffer;
  private final byte[] keyHashBytes;
  private final byte[] previousKeyHashBytes;
  private ByteBuffer previousKey = null;

  private final long[] hashIndex;

  private final HashPrefixCalculator prefixer;
  private int lastHashPrefix = -1;
  private int uncompressedOffset = 0;
  private int numEntriesInBlock = 0;

  private long numBytesWritten = 0;
  private long numRecordsWritten = 0;
  private long maxUncompressedBlockSize;
  private long maxCompressedBlockSize;

  public CueballWriter(OutputStream outputStream,
                       int keyHashSize,
                       Hasher hasher,
                       int valueSize,
                       CueballCompressionCodec compressionCodec,
                       int hashIndexBits) {
    // Buffer output
    this.stream = new BufferedOutputStream(outputStream, IOStreamUtils.DEFAULT_BUFFER_SIZE);
    this.keyHashSize = keyHashSize;
    this.hasher = hasher;
    this.valueSize = valueSize;
    this.compressionCodec = compressionCodec;

    uncompressedBuffer = new byte[(keyHashSize + valueSize)
        * DEFAULT_NUMBER_OF_ENTRIES];
    compressedBuffer = new byte[compressionCodec.getMaxCompressBufferSize(uncompressedBuffer.length)];
    keyHashBytes = new byte[keyHashSize];
    previousKeyHashBytes = new byte[keyHashSize];
    Arrays.fill(previousKeyHashBytes, (byte) 0);

    prefixer = new HashPrefixCalculator(hashIndexBits);

    hashIndex = new long[1 << hashIndexBits];
    Arrays.fill(hashIndex, -1);
  }

  @Override
  public void write(ByteBuffer key, ByteBuffer value) throws IOException {
    // Check that value size is compatible
    if (value.remaining() != valueSize) {
      throw new IOException("Size of value to be written is: "
          + value.remaining() + ", but configured value size is: " + valueSize);
    }
    // Check that key is different from previous one
    if (previousKey != null && previousKey.remaining() == key.remaining()
        && 0 == BytesUtils.compareBytesUnsigned(key, previousKey)) {
      throw new IOException("Keys must be distinct but two consecutive (in terms of comparableKey) keys are equal."
          + " Offending key: " + BytesUtils.bytesToHexString(key));
    }
    // Hash key
    hasher.hash(key, keyHashSize, keyHashBytes);
    // Compare with previous key hash
    int previousKeyHashComparision = BytesUtils.compareBytesUnsigned(keyHashBytes, 0, previousKeyHashBytes, 0, keyHashSize);
    // Check that there is not a key hash collision
    if (previousKey != null && 0 == previousKeyHashComparision) {
      throw new IOException("Collision: two consecutive keys have the same hash value."
          + "\nKey: "
          + BytesUtils.bytesToHexString(key)
          + "\nPrevious key: "
          + BytesUtils.bytesToHexString(previousKey)
          + "\nHash: "
          + BytesUtils.bytesToHexString(ByteBuffer.wrap(keyHashBytes)));
    }
    // Check key hash ordering
    if (0 > previousKeyHashComparision) {
      throw new IOException("Key ordering is incorrect. They should be ordered by increasing hash (comparableKey) value, but a decreasing sequence was detected."
          + "\nKey: "
          + BytesUtils.bytesToHexString(key)
          + "\nHash: "
          + BytesUtils.bytesToHexString(ByteBuffer.wrap(keyHashBytes))
          + "\nPrevious key: "
          + BytesUtils.bytesToHexString(previousKey)
          + "\nPrevious Hash: "
          + BytesUtils.bytesToHexString(ByteBuffer.wrap(previousKeyHashBytes)));
    }
    // Write hash
    writeHash(ByteBuffer.wrap(keyHashBytes), value);
    numRecordsWritten++;
    // Save current key and key hash
    System.arraycopy(keyHashBytes, 0, previousKeyHashBytes, 0, keyHashSize);
    previousKey = BytesUtils.byteBufferDeepCopy(key, previousKey);
  }

  public void writeHash(ByteBuffer hashedKey, ByteBuffer value) throws IOException {
    // check the first hashIndexBits of the hashedKey
    int thisPrefix = prefixer.getHashPrefix(hashedKey.array(), hashedKey.arrayOffset()
        + hashedKey.position());

    // if this prefix and the last one don't match, then it's time to clear the
    // buffer.
    if (lastHashPrefix == -1 || thisPrefix != lastHashPrefix) {
      // clear the uncompressed buffer and start over
      clearUncompressed();

      lastHashPrefix = thisPrefix;

      // record the start index of the next block
      hashIndex[thisPrefix] = numBytesWritten;
    }

    // at this point, we're guaranteed to be ready to write to the buffer.

    // write a subsequence of the key hash's bytes
    if (uncompressedOffset + keyHashSize > uncompressedBuffer.length) {
      throw new IOException("Out of room to write to uncompressed buffer for block "
          + Integer.toString(thisPrefix, 16)
          + "! Buffer size: "
          + uncompressedBuffer.length
          + ", offset: "
          + uncompressedOffset
          + ", hash size: "
          + keyHashSize
          + ", num entries written in block: "
          + numEntriesInBlock);
    }
    if (hashedKey.arrayOffset() + hashedKey.position() + keyHashSize > hashedKey.array().length) {
      throw new IOException("Need to copy " + keyHashSize
          + " from key, but there weren't enough bytes left! key buffer size: "
          + hashedKey.array().length + ", offset: " + hashedKey.arrayOffset()
          + hashedKey.position() + ", num entries written in block: "
          + numEntriesInBlock);
    }
    System.arraycopy(hashedKey.array(), hashedKey.arrayOffset()
        + hashedKey.position(), uncompressedBuffer, uncompressedOffset, keyHashSize);

    // encode the value offset and write it out
    System.arraycopy(value.array(), value.arrayOffset() + value.position(), uncompressedBuffer, uncompressedOffset
        + keyHashSize, valueSize);
    uncompressedOffset += keyHashSize + valueSize;
    ++numEntriesInBlock;
  }

  private void clearUncompressed() throws IOException {
    // compress the block
    int compressedSize = compressionCodec.compress(uncompressedBuffer, 0, uncompressedOffset, compressedBuffer, 0);
    // write the compressed block to the data stream
    stream.write(compressedBuffer, 0, compressedSize);
    numBytesWritten += compressedSize;

    // keep track of the max block sizes
    if (uncompressedOffset > maxUncompressedBlockSize) {
      maxUncompressedBlockSize = uncompressedOffset;
    }

    if (compressedSize > maxCompressedBlockSize) {
      maxCompressedBlockSize = compressedSize;
    }
    // Reset offset and counter
    uncompressedOffset = 0;
    numEntriesInBlock = 0;
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

    numBytesWritten += footer.length;

    // flush everything and close
    stream.flush();
    stream.close();
  }

  @Override
  public long getNumBytesWritten() {
    return numBytesWritten;
  }

  @Override
  public long getNumRecordsWritten() {
    return numRecordsWritten;
  }

  @Override
  public String toString() {
    return "CurlyWriter ["
        + "numRecordsWritten=" + getNumRecordsWritten()
        + ", numBytesWritten=" + getNumBytesWritten()
        + "]";
  }
}
