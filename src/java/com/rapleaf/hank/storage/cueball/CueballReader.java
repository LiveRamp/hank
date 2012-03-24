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

import com.rapleaf.hank.compress.CompressionCodec;
import com.rapleaf.hank.hasher.Hasher;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.ReaderResult;
import com.rapleaf.hank.util.AtomicLongCollection;
import com.rapleaf.hank.util.Bytes;
import com.rapleaf.hank.util.LruHashMap;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.SortedSet;

public class CueballReader implements Reader {

  private static final KeyHashBufferThreadLocal keyHashBufferThreadLocal = new KeyHashBufferThreadLocal();
  private static final int VALUE_CACHE_SIZE_LIMIT = 10000;
  private static final Logger LOG = Logger.getLogger(CueballReader.class);

  private final Hasher hasher;
  private final int valueSize;
  private final long[] hashIndex;
  private final FileChannel channel;
  private final int keyHashSize;
  private final int fullRecordSize;
  private final CompressionCodec compressionCodec;
  private int maxUncompressedBufferSize;
  private int maxCompressedBufferSize;
  private final HashPrefixCalculator prefixer;
  private final int versionNumber;
  private final LruHashMap<ByteBuffer, ByteBuffer> valueCache =
      new LruHashMap<ByteBuffer, ByteBuffer>(VALUE_CACHE_SIZE_LIMIT, VALUE_CACHE_SIZE_LIMIT);
  private final AtomicLongCollection cacheCounters = new AtomicLongCollection(2);

  public CueballReader(String partitionRoot,
                       int keyHashSize,
                       Hasher hasher,
                       int valueSize,
                       int hashIndexBits,
                       CompressionCodec compressionCodec) throws IOException {
    SortedSet<CueballFilePath> bases = Cueball.getBases(partitionRoot);
    if (bases == null || bases.size() == 0) {
      throw new IOException("Could not detect any Cueball base in " + partitionRoot);
    }
    CueballFilePath latestBase = bases.last();
    this.keyHashSize = keyHashSize;
    this.hasher = hasher;
    this.valueSize = valueSize;
    this.compressionCodec = compressionCodec;
    this.fullRecordSize = valueSize + keyHashSize;
    this.prefixer = new HashPrefixCalculator(hashIndexBits);
    this.versionNumber = latestBase.getVersion();

    channel = new FileInputStream(latestBase.getPath()).getChannel();
    Footer footer = new Footer(channel, hashIndexBits);
    hashIndex = footer.getHashIndex();
    maxUncompressedBufferSize = footer.getMaxUncompressedBufferSize();
    maxCompressedBufferSize = footer.getMaxCompressedBufferSize();
  }

  @Override
  public void get(ByteBuffer key, ReaderResult result) throws IOException {
    // Note: keyHash buffer might be larger than keyHashSize
    byte[] keyHash = computeKeyHash(key);
    ByteBuffer keyHashByteBuffer = ByteBuffer.wrap(keyHash);

    if (loadValueFromCache(keyHashByteBuffer, result)) {
      return;
    }

    // We will read the compressed buffer and decompress it in the same buffer.
    result.requiresBufferSize(maxCompressedBufferSize + maxUncompressedBufferSize);

    int hashPrefix = prefixer.getHashPrefix(keyHash, 0);
    long baseOffset = hashIndex[hashPrefix];

    // by default, we didn't find what we were looking for
    result.notFound();

    // baseOffset of -1 means that our hashPrefix doesn't map to any blocks
    if (baseOffset >= 0) {
      // set up to read a chunk from the datafile
      ByteBuffer buffer = result.getBuffer();
      buffer.rewind();
      buffer.limit(maxCompressedBufferSize);
      int bytesRead = channel.read(buffer, baseOffset);

      // decompress from the beginning of the buffer into the unoccupied end of
      // the buffer
      final int uncompressedStart = bytesRead;
      int decompressedLength = compressionCodec.decompress(buffer.array(),
          0,
          bytesRead, buffer.array(),
          uncompressedStart);

      // scan the chunk we read to find a matching key, if there is one,
      // returning the recordfile offset
      int bufferOffset = getValueOffset(buffer.array(),
          uncompressedStart,
          uncompressedStart + decompressedLength,
          keyHash);

      // -1 means that we didn't find the key
      if (bufferOffset > -1) {
        result.found();
        buffer.limit(bufferOffset + valueSize);
        buffer.position(bufferOffset);
        addValueToCache(keyHashByteBuffer, buffer);
      }
    }
  }

  public Integer getVersionNumber() {
    return versionNumber;
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  private int getValueOffset(byte[] keyfileBufferChunk, int off, int limit, byte[] key) {
    for (; off < limit; off += fullRecordSize) {
      int comparison = Bytes.compareBytesUnsigned(keyfileBufferChunk, off,
          key, 0, keyHashSize);
      // found match
      if (comparison == 0) {
        return off + keyHashSize;
      }

      // passed the spot where our key could have been found, so not going to
      // find it
      if (comparison == 1) {
        break;
      }
    }
    // looked everywhere, didn't find it!
    return -1;
  }

  private static class KeyHashBufferThreadLocal extends ThreadLocal<byte[]> {

    private static int KEY_HASH_BUFFER_INITIAL_SIZE = 8;

    @Override
    protected byte[] initialValue() {
      return new byte[KEY_HASH_BUFFER_INITIAL_SIZE];
    }

    protected byte[] getAndRequireBufferSize(int size) {
      byte[] buffer = this.get();
      if (buffer.length < size) {
        buffer = new byte[size];
        this.set(buffer);
      }
      return buffer;
    }
  }

  // Note: result buffer might be larger than keyHashSize
  private byte[] computeKeyHash(ByteBuffer key) {
    // Reuse a thread local buffer, but first make sure it is at least of the required size
    byte[] keyHash = keyHashBufferThreadLocal.getAndRequireBufferSize(keyHashSize);
    hasher.hash(key, keyHashSize, keyHash);
    return keyHash;
  }

  private void addValueToCache(ByteBuffer keyHash, ByteBuffer value) {
    synchronized (valueCache) {
      valueCache.put(Bytes.byteBufferDeepCopy(keyHash), Bytes.byteBufferDeepCopy(value));
    }
  }

  private boolean loadValueFromCache(ByteBuffer keyHash, ReaderResult result) {
    ByteBuffer value;
    synchronized (valueCache) {
      value = valueCache.get(keyHash);
    }
    if (value != null) {
      cacheCounters.increment(1, 1);
    } else {
      cacheCounters.increment(1, 0);
    }
    if (cacheCounters.get(0) > 200) {
      long[] values = cacheCounters.getAsArrayAndSet(0, 0);
      synchronized (valueCache) {
        LOG.info("Requests found in cache (CUEBALL): " + values[1] + "/" + values[0] + "(" + ((double) values[1] / (double) values[0]) * 100 + ") cache size: " + valueCache.size());
      }
    }
    return false;
  }

}
