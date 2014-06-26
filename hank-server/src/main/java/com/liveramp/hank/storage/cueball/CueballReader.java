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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.SortedSet;

import com.liveramp.commons.util.ByteBufferMemoryUsageEstimator;
import com.liveramp.commons.util.BytesUtils;
import com.liveramp.hank.compression.cueball.CueballCompressionCodec;
import com.liveramp.hank.hasher.Hasher;
import com.liveramp.hank.storage.CacheStatistics;
import com.liveramp.hank.storage.Reader;
import com.liveramp.hank.storage.ReaderResult;
import com.liveramp.hank.util.SynchronizedMemoryBoundCache;

public class CueballReader implements Reader {

  private static final KeyHashBufferThreadLocal keyHashBufferThreadLocal = new KeyHashBufferThreadLocal();
  private static final ByteBuffer NOT_FOUND_MARKER = ByteBuffer.wrap(new byte[]{});

  private final Hasher hasher;
  private final int valueSize;
  private final long[] hashIndex;
  private final FileChannel channel;
  private final int keyHashSize;
  private final int fullRecordSize;
  private final CueballCompressionCodec compressionCodec;
  private int maxUncompressedBufferSize;
  private int maxCompressedBufferSize;
  private final HashPrefixCalculator prefixer;
  private final int versionNumber;
  private SynchronizedMemoryBoundCache<ByteBuffer, ByteBuffer> cache;

  public CueballReader(String partitionRoot,
                       int keyHashSize,
                       Hasher hasher,
                       int valueSize,
                       int hashIndexBits,
                       CueballCompressionCodec compressionCodec,
                       long cacheNumBytesCapacity,
                       int cacheNumItemsCapacity) throws IOException {
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
    cache = new SynchronizedMemoryBoundCache<ByteBuffer, ByteBuffer>(
        cacheNumBytesCapacity > 0 || cacheNumItemsCapacity > 0,
        cacheNumBytesCapacity,
        cacheNumItemsCapacity,
        new ByteBufferMemoryUsageEstimator(),
        new ByteBufferMemoryUsageEstimator());
  }

  @Override
  public void get(ByteBuffer key, ReaderResult result) throws IOException {
    // Note: keyHash buffer might be larger than keyHashSize
    byte[] keyHash = computeKeyHash(key);
    ByteBuffer keyHashByteBuffer = ByteBuffer.wrap(keyHash);

    int hashPrefix = prefixer.getHashPrefix(keyHash, 0);
    long baseOffset = hashIndex[hashPrefix];

    // by default, we didn't find what we were looking for
    result.notFound();

    // baseOffset of -1 means that our hashPrefix doesn't map to any blocks
    if (baseOffset >= 0) {
      // Attempt to load value from the cache
      if (loadValueFromCache(keyHashByteBuffer, result)) {
        return;
      }
      // We will read the compressed buffer and decompress it in the same buffer.
      result.requiresBufferSize(maxCompressedBufferSize + maxUncompressedBufferSize);
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
      } else {
        // key not found
        addNotFoundToCache(keyHashByteBuffer);
      }
    }
  }

  public Integer getVersionNumber() {
    return versionNumber;
  }

  @Override
  public CacheStatistics getCacheStatistics() {
    return new CacheStatistics(
        cache.size(),
        cache.getMaxNumItems(),
        cache.getNumManagedBytes(),
        cache.getMaxNumManagedBytes());
  }

  @Override
  public void close() throws IOException {
    channel.close();
    cache = null;
  }

  private int getValueOffset(byte[] keyfileBufferChunk, int off, int limit, byte[] key) {
    for (; off < limit; off += fullRecordSize) {
      int comparison = BytesUtils.compareBytesUnsigned(keyfileBufferChunk, off,
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
    cache.put(BytesUtils.byteBufferDeepCopy(keyHash), BytesUtils.byteBufferDeepCopy(value));
  }

  private void addNotFoundToCache(ByteBuffer keyHash) {
    cache.put(BytesUtils.byteBufferDeepCopy(keyHash), NOT_FOUND_MARKER);
  }

  // Return true if managed to read the corresponding value from the cache and into result
  private boolean loadValueFromCache(ByteBuffer keyHash, ReaderResult result) {
    ByteBuffer value;
    value = cache.get(keyHash);
    if (value != null) {
      // Compare against the not found marker (note that this is an address equality
      // and not an object equality on purpose)
      if (value != NOT_FOUND_MARKER) {
        // Load cached value into result
        result.deepCopyIntoResultBuffer(value);
        result.found();
      } else {
        result.notFound();
      }
      // Was found in cache
      result.setL1CacheHit(true);
      return true;
    } else {
      // Was not found in cache
      return false;
    }
  }
}
