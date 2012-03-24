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
import com.rapleaf.hank.util.AtomicLongCollection;
import com.rapleaf.hank.util.Bytes;
import com.rapleaf.hank.util.EncodingHelper;
import com.rapleaf.hank.util.LruHashMap;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.SortedSet;

public class CurlyReader implements Reader, ICurlyReader {

  private static final int VALUE_CACHE_SIZE_LIMIT = 10000;
  private static final Logger LOG = Logger.getLogger(CurlyReader.class);

  private final Reader keyFileReader;
  private final int readBufferSize;
  private final FileChannel recordFile;
  private final int versionNumber;
  private final LruHashMap<Long, ByteBuffer> valueCache =
      new LruHashMap<Long, ByteBuffer>(VALUE_CACHE_SIZE_LIMIT, VALUE_CACHE_SIZE_LIMIT);
  private final AtomicLongCollection cacheCounters = new AtomicLongCollection(2);

  public CurlyReader(String partitionRoot, int recordFileReadBufferBytes, Reader keyFileReader)
      throws IOException {
    SortedSet<CurlyFilePath> bases = Curly.getBases(partitionRoot);
    if (bases == null || bases.size() == 0) {
      throw new IOException("Could not detect any Curly base in " + partitionRoot);
    }
    CurlyFilePath latestBase = bases.last();
    this.recordFile = new FileInputStream(latestBase.getPath()).getChannel();
    this.keyFileReader = keyFileReader;
    this.readBufferSize = recordFileReadBufferBytes;
    this.versionNumber = latestBase.getVersion();

  }

  public CurlyReader(CurlyFilePath curlyFile, int recordFileReadBufferBytes, Reader keyFileReader)
      throws FileNotFoundException {
    this.recordFile = new FileInputStream(curlyFile.getPath()).getChannel();
    this.keyFileReader = keyFileReader;
    this.readBufferSize = recordFileReadBufferBytes;
    this.versionNumber = curlyFile.getVersion();
  }


  // Note: the buffer in result must be at least readBufferSize long
  @Override
  public void readRecordAtOffset(long recordFileOffset, ReaderResult result) throws IOException {
    if (loadValueFromCache(recordFileOffset, result)) {
      return;
    }
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
    addValueToCache(recordFileOffset, result.getBuffer());
  }

  private void addValueToCache(long recordFileOffset, ByteBuffer value) {
    synchronized (valueCache) {
      valueCache.put(recordFileOffset, Bytes.byteBufferDeepCopy(value));
    }
  }

  private boolean loadValueFromCache(long recordFileOffset, ReaderResult result) {
    ByteBuffer value;
    synchronized (valueCache) {
      value = valueCache.get(recordFileOffset);
    }
    if (value != null) {
      cacheCounters.increment(1, 1);
    } else {
      cacheCounters.increment(1, 0);
    }
    if (cacheCounters.get(0) > 5000) {
      long[] values = cacheCounters.getAsArrayAndSet(0, 0);
      synchronized (valueCache) {
        LOG.info("Requests found in cache (CURLY): " + values[1] + "/" + values[0] + "(" + ((double) values[1] / (double) values[0]) * 100 + ") cache size: " + valueCache.size());
      }
    }
    return false;
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
      // the result buffer contains the offset in the record file. decode it.
      long recordFileOffset = EncodingHelper.decodeLittleEndianFixedWidthLong(result.getBuffer());
      // now we know where to look
      readRecordAtOffset(recordFileOffset, result);
    }
  }

  public Integer getVersionNumber() {
    return versionNumber;
  }

  @Override
  public void close() throws IOException {
    if (recordFile != null) {
      recordFile.close();
    }
    if (keyFileReader != null) {
      keyFileReader.close();
    }
  }
}
