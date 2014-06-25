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
package com.liveramp.hank.storage.curly;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.liveramp.commons.collections.LruHashMap;
import com.liveramp.commons.util.BytesUtils;
import com.liveramp.hank.compression.CompressionCodec;
import com.liveramp.hank.hasher.Murmur64Hasher;
import com.liveramp.hank.storage.Writer;
import com.liveramp.hank.util.EncodingHelper;
import com.liveramp.hank.util.IOStreamUtils;

public class CurlyWriter implements Writer {

  private static final int VALUE_FOLDING_HASH_NUM_BYTES = 16;

  private static final Murmur64Hasher murmur64Hasher = new Murmur64Hasher();

  private long currentRecordOffset;
  private long numFoldedValues = 0;
  private long numFoldedBytesApproximate = 0;

  private final Writer keyfileWriter;
  private final OutputStream recordFileStream;
  private final int offsetNumBytes;
  private final long maxOffset;
  private final ByteBuffer valueOffsetBuffer;
  private final byte[] valueLengthBuffer = new byte[5];

  // Compression
  private final CompressionCodec blockCompressionCodec;
  private ByteArrayOutputStream compressedBlockOutputStream;
  private OutputStream compressionOutputStream;
  private final int compressedBlockSizeThreshold;
  private final int offsetInBlockNumBytes;
  private int offsetInDecompressedBlock = 0;

  // Cache
  private final LruHashMap<ByteBuffer, ByteBuffer> hashedValueToEncodedRecordOffsetCache;

  public CurlyWriter(OutputStream recordfileStream,
                     Writer keyfileWriter,
                     int offsetNumBytes,
                     int valueFoldingCacheCapacity) throws IOException {
    this(recordfileStream, keyfileWriter, offsetNumBytes, valueFoldingCacheCapacity, null, -1, -1);
  }

  public CurlyWriter(OutputStream recordfileStream,
                     Writer keyfileWriter,
                     int offsetNumBytes,
                     int valueFoldingCacheCapacity,
                     CompressionCodec blockCompressionCodec,
                     int compressedBlockSizeThreshold,
                     int offsetInBlockNumBytes) throws IOException {
    // Buffer output
    this.recordFileStream = new BufferedOutputStream(recordfileStream, IOStreamUtils.DEFAULT_BUFFER_SIZE);
    this.keyfileWriter = keyfileWriter;
    this.blockCompressionCodec = blockCompressionCodec;
    this.offsetNumBytes = offsetNumBytes;
    this.maxOffset = 1L << (offsetNumBytes * 8);
    this.currentRecordOffset = 0;
    this.compressedBlockSizeThreshold = compressedBlockSizeThreshold;
    this.offsetInBlockNumBytes = offsetInBlockNumBytes;

    // Initialize LRU cache only when needed
    if (valueFoldingCacheCapacity > 0) {
      hashedValueToEncodedRecordOffsetCache = new LruHashMap<ByteBuffer, ByteBuffer>(valueFoldingCacheCapacity, valueFoldingCacheCapacity);
    } else {
      hashedValueToEncodedRecordOffsetCache = null;
    }

    if (blockCompressionCodec == null) {
      // No block compression
      valueOffsetBuffer = ByteBuffer.wrap(new byte[offsetNumBytes]);
      compressedBlockOutputStream = null;
      compressionOutputStream = null;
    } else {
      // Initialize block compression
      valueOffsetBuffer = ByteBuffer.wrap(new byte[offsetNumBytes + offsetInBlockNumBytes]);
      compressedBlockOutputStream = new ByteArrayOutputStream();
      compressionOutputStream = null;
    }
  }

  @Override
  public void close() throws IOException {
    if (blockCompressionCodec != null) {
      flushCompressedBlock();
    }
    recordFileStream.flush();
    recordFileStream.close();
    keyfileWriter.close();
    if (hashedValueToEncodedRecordOffsetCache != null) {
      hashedValueToEncodedRecordOffsetCache.clear();
    }
  }

  @Override
  public void write(ByteBuffer key, ByteBuffer value) throws IOException {
    if (currentRecordOffset > maxOffset) {
      throw new IOException("Exceeded configured max recordfile size of "
          + maxOffset
          + ". Increase number of partitions to go back below this level.");
    }

    ByteBuffer cachedValueRecordEncodedOffset = null;
    ByteBuffer hashedValue = null;

    // Retrieve cached offset if possible
    if (hashedValueToEncodedRecordOffsetCache != null) {
      hashedValue = computeHash(value);
      cachedValueRecordEncodedOffset = hashedValueToEncodedRecordOffsetCache.get(hashedValue);
    }

    if (cachedValueRecordEncodedOffset != null) {
      // Write cached offset in key file and nothing else needs to be done
      keyfileWriter.write(key, cachedValueRecordEncodedOffset);
      numFoldedValues += 1;
      numFoldedBytesApproximate += value.remaining();
    } else {
      if (blockCompressionCodec == null) {
        //
        // Uncompressed mode
        //
        EncodingHelper.encodeLittleEndianFixedWidthLong(currentRecordOffset, valueOffsetBuffer.array());
        // Write current offset in key file
        keyfileWriter.write(key, valueOffsetBuffer);
        // Value was not found in cache. Cache current value encoded offset buffer if needed
        if (hashedValueToEncodedRecordOffsetCache != null) {
          hashedValueToEncodedRecordOffsetCache.put(hashedValue, BytesUtils.byteBufferDeepCopy(valueOffsetBuffer));
        }
        // Encode value size and write it
        int valueLength = value.remaining();
        int valueLengthNumBytes = EncodingHelper.encodeLittleEndianVarInt(valueLength, valueLengthBuffer);
        recordFileStream.write(valueLengthBuffer, 0, valueLengthNumBytes);
        currentRecordOffset += valueLengthNumBytes;
        // Write value
        recordFileStream.write(value.array(), value.arrayOffset() + value.position(), valueLength);
        currentRecordOffset += valueLength;
      } else {
        //
        // Block compression mode
        //

        // Flush the compressed block if needed
        if (compressedBlockOutputStream.size() >= compressedBlockSizeThreshold) {
          flushCompressedBlock();
        }
        // Create a new compression output stream if needed (either this is the first value or we just flushed)
        if (compressionOutputStream == null) {
          initStreams();
        }

        // Encode value size and write it to compressed block
        int valueLength = value.remaining();
        int valueLengthNumBytes = EncodingHelper.encodeLittleEndianVarInt(valueLength, valueLengthBuffer);
        compressionOutputStream.write(valueLengthBuffer, 0, valueLengthNumBytes);
        // Write value to compressed block
        compressionOutputStream.write(value.array(), value.arrayOffset() + value.position(), valueLength);
        // Flush compression output stream so that its content is immediately in the output stream and we can check the size
        compressionOutputStream.flush();
        // Create the offset and index buffer corresponding to this value
        EncodingHelper.encodeLittleEndianFixedWidthLong(currentRecordOffset, valueOffsetBuffer.array(), 0, offsetNumBytes);
        EncodingHelper.encodeLittleEndianFixedWidthLong(offsetInDecompressedBlock, valueOffsetBuffer.array(), offsetNumBytes, offsetInBlockNumBytes);
        // Write to key file
        keyfileWriter.write(key, valueOffsetBuffer);
        // Value was not found in cache. Cache current value encoded offset buffer if needed
        if (hashedValueToEncodedRecordOffsetCache != null) {
          hashedValueToEncodedRecordOffsetCache.put(hashedValue, BytesUtils.byteBufferDeepCopy(valueOffsetBuffer));
        }
        // Increment the offset
        offsetInDecompressedBlock += valueLengthNumBytes + valueLength;
      }
    }
  }

  private void initStreams() throws IOException {
    // Reset the byte array output stream and the offset in it
    compressedBlockOutputStream.reset();
    offsetInDecompressedBlock = 0;
    // Initialize new compression stream
    compressionOutputStream = blockCompressionCodec.getFactory().getCompressor().getOutputStream(compressedBlockOutputStream);
  }

  private void flushCompressedBlock() throws IOException {
    if (compressionOutputStream != null) {
      // First, close compression output stream so that they write all data
      compressionOutputStream.close();
      compressionOutputStream = null;
      // Encode compressed block size and write it to record stream
      int valueLengthNumBytes = EncodingHelper.encodeLittleEndianVarInt(compressedBlockOutputStream.size(), valueLengthBuffer);
      recordFileStream.write(valueLengthBuffer, 0, valueLengthNumBytes);
      currentRecordOffset += valueLengthNumBytes;
      // Write compressed block to record stream
      compressedBlockOutputStream.writeTo(recordFileStream);
      currentRecordOffset += compressedBlockOutputStream.size();
    }
  }

  private ByteBuffer computeHash(ByteBuffer value) {
    // 128-bit murmur64 hash
    byte[] hashBytes = new byte[VALUE_FOLDING_HASH_NUM_BYTES];
    murmur64Hasher.hash(value, VALUE_FOLDING_HASH_NUM_BYTES, hashBytes);
    return ByteBuffer.wrap(hashBytes);
  }

  @Override
  public long getNumBytesWritten() {
    return keyfileWriter.getNumBytesWritten() + currentRecordOffset;
  }

  @Override
  public long getNumRecordsWritten() {
    return keyfileWriter.getNumRecordsWritten();
  }

  @Override
  public String toString() {
    return "CurlyWriter [keyFileWriter=" + keyfileWriter.toString()
        + ", numRecordsWritten=" + getNumRecordsWritten()
        + ", numBytesWritten=" + getNumBytesWritten()
        + ", numFoldedValues=" + numFoldedValues
        + ", numFoldedBytesApproximate=" + numFoldedBytesApproximate
        + ", blockCompressionCodec=" + blockCompressionCodec
        + ", compressedBlockSizeThreshold=" + compressedBlockSizeThreshold
        + ", offsetInBlockNumBytes=" + offsetInBlockNumBytes
        + "]";
  }
}
