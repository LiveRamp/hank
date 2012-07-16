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

import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.ReaderResult;
import com.rapleaf.hank.util.Bytes;
import com.rapleaf.hank.util.EncodingHelper;
import com.rapleaf.hank.util.LruHashMap;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.SortedSet;
import java.util.zip.GZIPInputStream;

public class CurlyReader implements Reader, ICurlyReader {

  private final Reader keyFileReader;
  private final int readBufferSize;
  private final FileChannel recordFile;
  private final int versionNumber;
  private final LruHashMap<ByteBuffer, ByteBuffer> cache;
  private final BlockCompressionCodec blockCompressionCodec;
  private final int offsetNumBytes;
  private final int offsetInBlockNumBytes;

  public static CurlyFilePath getLatestBase(String partitionRoot) throws IOException {
    SortedSet<CurlyFilePath> bases = Curly.getBases(partitionRoot);
    if (bases == null || bases.size() == 0) {
      throw new IOException("Could not detect any Curly base in " + partitionRoot);
    }
    return bases.last();
  }

  public CurlyReader(CurlyFilePath curlyFile,
                     int recordFileReadBufferBytes,
                     Reader keyFileReader,
                     int cacheCapacity) throws IOException {
    this(curlyFile, recordFileReadBufferBytes, keyFileReader, cacheCapacity, null, -1, -1);
  }

  public CurlyReader(CurlyFilePath curlyFile,
                     int recordFileReadBufferBytes,
                     Reader keyFileReader,
                     int cacheCapacity,
                     BlockCompressionCodec blockCompressionCodec,
                     int offsetNumBytes,
                     int offsetInBlockNumBytes) throws IOException {
    this.recordFile = new FileInputStream(curlyFile.getPath()).getChannel();
    this.keyFileReader = keyFileReader;
    this.readBufferSize = recordFileReadBufferBytes;
    this.versionNumber = curlyFile.getVersion();
    this.blockCompressionCodec = blockCompressionCodec;
    this.offsetNumBytes = offsetNumBytes;
    this.offsetInBlockNumBytes = offsetInBlockNumBytes;
    if (cacheCapacity > 0) {
      this.cache = new LruHashMap<ByteBuffer, ByteBuffer>(0, cacheCapacity);
    } else {
      this.cache = null;
    }
    // Check that key file is at the same version
    if (keyFileReader != null &&
        keyFileReader.getVersionNumber() != null &&
        !keyFileReader.getVersionNumber().equals(versionNumber)) {
      throw new IOException("Curly Reader version (" + versionNumber
          + ") does not match the provided key file Reader version (" + keyFileReader.getVersionNumber() + ")");
    }
  }

  @Override
  // Note: the buffer in result must be at least readBufferSize long
  public void readRecord(ByteBuffer location, ReaderResult result) throws IOException {
    // Attempt to load value from the cache
    if (cache != null && loadValueFromCache(location, result)) {
      return;
    }
    // Deep copy the location if caching is activated
    ByteBuffer locationDeepCopy = cache != null ? Bytes.byteBufferDeepCopy(location) : null;
    if (blockCompressionCodec == null) {
      // When not using block compression, location just contains an offset. Decode it.
      long recordFileOffset = EncodingHelper.decodeLittleEndianFixedWidthLong(location);
      // Directly read record into result
      readRecordAtOffset(recordFileOffset, result);
    } else {
      // When using block compression, location contains the block's offset and an offset in the block. Decode them.
      long recordFileOffset = EncodingHelper.decodeLittleEndianFixedWidthLong(location.array(),
          location.arrayOffset() + location.position(), offsetNumBytes);
      long offsetInBlock = EncodingHelper.decodeLittleEndianFixedWidthLong(location.array(),
          location.arrayOffset() + location.position() + offsetNumBytes, offsetInBlockNumBytes);
      // Read in the compressed block into the result
      readRecordAtOffset(recordFileOffset, result);

      // Decompress the block
      InputStream blockInputStream = new ByteArrayInputStream(result.getBuffer().array(),
          result.getBuffer().arrayOffset() + result.getBuffer().position(), result.getBuffer().remaining());
      // Build an InputStream corresponding to the compression codec
      InputStream decompressedBlockInputStream;
      switch (blockCompressionCodec) {
        case GZIP:
          decompressedBlockInputStream = new GZIPInputStream(blockInputStream);
          break;
        case SLOW_IDENTITY:
          decompressedBlockInputStream = new BufferedInputStream(blockInputStream);
          break;
        default:
          throw new RuntimeException("Unknown block compression codec: " + blockCompressionCodec);
      }
      // Decompress into the specialized result buffer
      IOUtils.copy(decompressedBlockInputStream, result.getDecompressionOutputStream());
      ByteBuffer decompressedBlockByteBuffer = result.getDecompressionOutputStream().getByteBuffer();

      // Position ourselves at the beginning of the actual value
      decompressedBlockByteBuffer.position((int) offsetInBlock);
      // Determine result value size
      int valueSize = EncodingHelper.decodeLittleEndianVarInt(decompressedBlockByteBuffer);

      // We can exactly wrap our value
      ByteBuffer value = ByteBuffer.wrap(decompressedBlockByteBuffer.array(),
          decompressedBlockByteBuffer.arrayOffset() + decompressedBlockByteBuffer.position(), valueSize);

      // Copy decompressed result into final result buffer
      result.requiresBufferSize(valueSize);
      result.getBuffer().clear();
      result.getBuffer().put(value);
      result.getBuffer().flip();
    }
    // Store result in cache if needed
    if (cache != null) {
      addValueToCache(locationDeepCopy, result.getBuffer());
    }
  }

  // Note: the buffer in result must be at least readBufferSize long
  private void readRecordAtOffset(long recordFileOffset, ReaderResult result) throws IOException {
    // Let's reset the buffer so we can do our read.
    result.getBuffer().rewind();
    // the buffer is already at least this big, so we'll extend it back out.
    result.getBuffer().limit(readBufferSize);

    // TODO: it does seem like there's a chance that this could return too few
    // bytes to do the varint decoding.
    recordFile.read(result.getBuffer(), recordFileOffset);
    result.getBuffer().rewind();
    int recordSize = EncodingHelper.decodeLittleEndianVarInt(result.getBuffer());

    // now we know how many bytes to read. do the second read to get the data.

    int bytesInRecordSize = result.getBuffer().position();

    // we may already have read the entire value in during our first read. we
    // can tell this if the remainin() is >= the record size.
    if (result.getBuffer().remaining() < recordSize) {
      // hm, looks like we didn't read the whole value the first time. bummer.
      // the good news is that we *do* know how much to read this time. the
      // new size we select is big enough to hold this value and its varint
      // size. note that we won't actually be reading the varint part again -
      // we only do this size adjustment to help prevent the next under-read
      // from requiring a buffer resize.
      int newSize = recordSize + EncodingHelper.MAX_VARINT_SIZE;
      // resize the buffer
      result.requiresBufferSize(newSize);
      result.getBuffer().position(0);
      result.getBuffer().limit(recordSize);

      // read until we've either run out of bytes or gotten the entire record
      int bytesRead = 0;
      while (bytesRead < recordSize) {
        // since we're using the stateless version of read(), we have to keep
        // moving the offset pointer ourselves
        int bytesReadTemp = recordFile.read(result.getBuffer(), recordFileOffset
            + bytesInRecordSize + bytesRead);

        if (bytesReadTemp == -1) {
          // hm, actually, i think this is an error case!
          break;
        }

        bytesRead += bytesReadTemp;
      }
      // we're done reading, so position back to beginning of buffer
      result.getBuffer().position(0);
    }
    // the value should start at buffer.position() and go for recordSize
    // bytes, so limit it appropriately.
    result.getBuffer().limit(recordSize + result.getBuffer().position());
  }

  @Override
  public void get(ByteBuffer key, ReaderResult result) throws IOException {
    // we want at least readBufferSize bytes of available space. we might resize
    // again later.
    result.requiresBufferSize(readBufferSize);

    // ask the keyfile for this key
    keyFileReader.get(key, result);

    // if the key is found, then we are prepared to do the second lookup.
    if (result.isFound()) {
      // now we know where to look
      readRecord(result.getBuffer(), result);
    }
  }

  @Override
  public Integer getVersionNumber() {
    return versionNumber;
  }

  // Note: location should already be a deep copy that won't get modified
  private void addValueToCache(ByteBuffer location, ByteBuffer value) {
    synchronized (cache) {
      cache.put(location, Bytes.byteBufferDeepCopy(value));
    }
  }

  // Return true if managed to read the corresponding value from the cache and into result
  private boolean loadValueFromCache(ByteBuffer location, ReaderResult result) {
    ByteBuffer value;
    synchronized (cache) {
      value = cache.get(location);
    }
    if (value != null) {
      result.deepCopyIntoResultBuffer(value);
      result.found();
      result.setL2CacheHit(true);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void close() throws IOException {
    if (recordFile != null) {
      recordFile.close();
    }
    if (keyFileReader != null) {
      keyFileReader.close();
    }
    if (cache != null) {
      cache.clear();
    }
  }
}
