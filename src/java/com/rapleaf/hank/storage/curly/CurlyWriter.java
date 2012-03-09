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
package com.rapleaf.hank.storage.curly;

import com.rapleaf.hank.hasher.Murmur64Hasher;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.util.Bytes;
import com.rapleaf.hank.util.EncodingHelper;
import com.rapleaf.hank.util.LruHashMap;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class CurlyWriter implements Writer {

  private static final int MURMUR_64_SEED = 1395834302;

  private final Writer keyfileWriter;
  private final OutputStream recordFileStream;

  private long currentRecordOffset;
  private final long maxOffset;
  private final ByteBuffer valueOffsetBuffer;
  private final byte[] valueLengthBuffer = new byte[5];
  private final LruHashMap<Long, ByteBuffer> hashedValueToEncodedRecordOffsetCache;

  public CurlyWriter(OutputStream recordfileStream,
                     Writer keyfileWriter,
                     int offsetSize,
                     int valueFoldingCacheSize) {
    this.recordFileStream = recordfileStream;
    this.keyfileWriter = keyfileWriter;
    this.maxOffset = 1L << (offsetSize * 8);
    this.currentRecordOffset = 0;

    valueOffsetBuffer = ByteBuffer.wrap(new byte[offsetSize]);

    // Initialize LRU cache only when needed
    if (valueFoldingCacheSize > 0) {
      hashedValueToEncodedRecordOffsetCache = new LruHashMap<Long, ByteBuffer>(valueFoldingCacheSize, valueFoldingCacheSize);
    } else {
      hashedValueToEncodedRecordOffsetCache = null;
    }
  }

  @Override
  public void close() throws IOException {
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
    Long hashedValue = null;

    // Retrieve cached offset if possible
    if (hashedValueToEncodedRecordOffsetCache != null) {
      hashedValue = Murmur64Hasher.murmurHash64(value.array(), value.arrayOffset() + value.position(),
          value.remaining(), MURMUR_64_SEED);
      cachedValueRecordEncodedOffset = hashedValueToEncodedRecordOffsetCache.get(hashedValue);
    }

    if (cachedValueRecordEncodedOffset != null) {
      // Write cached offset in key file and nothing else needs to be done
      keyfileWriter.write(key, cachedValueRecordEncodedOffset);
    } else {
      EncodingHelper.encodeLittleEndianFixedWidthLong(currentRecordOffset, valueOffsetBuffer.array());
      // Write current offset in key file
      keyfileWriter.write(key, valueOffsetBuffer);
      // Value was not found in cache. Cache current value encoded offset buffer if needed
      if (hashedValueToEncodedRecordOffsetCache != null) {
        hashedValueToEncodedRecordOffsetCache.put(hashedValue, Bytes.byteBufferDeepCopy(valueOffsetBuffer));
      }
      int valueLength = value.remaining();
      int valueLengthNumBytes = EncodingHelper.encodeLittleEndianVarInt(valueLength, valueLengthBuffer);
      // Write var int representing value length
      recordFileStream.write(valueLengthBuffer, 0, valueLengthNumBytes);
      // Write value
      recordFileStream.write(value.array(), value.arrayOffset() + value.position(), valueLength);
      // Advance record offset
      currentRecordOffset += valueLengthNumBytes + valueLength;
    }
  }

  @Override
  public long getNumBytesWritten() {
    return keyfileWriter.getNumBytesWritten() + currentRecordOffset;
  }

  @Override
  public long getNumRecordsWritten() {
    return keyfileWriter.getNumRecordsWritten();
  }
}
