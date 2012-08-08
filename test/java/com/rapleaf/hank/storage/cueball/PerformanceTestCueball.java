/**
 *  Copyright 2012 Rapleaf
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

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.compress.NoCompressionCodec;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;
import com.rapleaf.hank.hasher.Hasher;
import com.rapleaf.hank.performance.HankTimer;
import com.rapleaf.hank.storage.LocalPartitionRemoteFileOps;
import com.rapleaf.hank.storage.PartitionRemoteFileOps;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.rapleaf.hank.util.EncodingHelper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

public class PerformanceTestCueball extends BaseTestCase {

  private static final int VALUE_SIZE = 16;
  private static final int KEY_SIZE = 20;
  private static final int KEY_HASH_SIZE = KEY_SIZE;
  private static final int HASH_INDEX_BITS = 16;
  private static final int NUM_RECORDS_PER_BLOCK = 1000;

  private Cueball getCueball() {
    return new Cueball(
        KEY_HASH_SIZE, new KeyHasher(HASH_INDEX_BITS), VALUE_SIZE, HASH_INDEX_BITS, localTmpDir + "/remote_domain_root",
        new LocalPartitionRemoteFileOps.Factory(), NoCompressionCodec.class, null, 0, -1);
  }

  private long getNumTotalRecords() {
    return (1 << HASH_INDEX_BITS) * NUM_RECORDS_PER_BLOCK;
  }

  public void testPerformanceCueballWriter() throws IOException {
    // Fill in all indexable blocks
    long numRecords = getNumTotalRecords();
    Writer writer = getCueball().getWriter(
        new MockDomainVersion(0, 0L, new IncrementalDomainVersionProperties.Base()),
        new LocalPartitionRemoteFileOps(localTmpDir, 0), 0);
    HankTimer timer = new HankTimer();
    for (long i = 0; i < numRecords; ++i) {
      writer.write(key(i, KEY_SIZE), value(i, VALUE_SIZE));
    }
    writer.close();
    double elapsedMs = timer.getDurationMs();
    double elapsedSecs = elapsedMs / 1000.0;
    long totalBytes = numRecords * (KEY_SIZE + VALUE_SIZE);
    long totalMegaBytes = (totalBytes / 1024 / 1024);
    System.out.println("Test took " + elapsedMs + "ms, wrote " + numRecords + " records totalling " + totalMegaBytes + "MB");
    System.out.println(String.format("Throughput: %.2f writes/sec", numRecords / elapsedSecs));
    System.out.println(String.format("Throughput: %.2f MB/sec", totalMegaBytes / elapsedSecs));
  }

  public void testPerformanceCueballMerger() throws IOException {
    DomainVersion baseVersion = new MockDomainVersion(0, 0L, new IncrementalDomainVersionProperties.Base());
    DomainVersion deltaVersion = new MockDomainVersion(1, 0L, new IncrementalDomainVersionProperties.Delta(0));
    DomainVersion newBaseVersion = new MockDomainVersion(1, 0L, new IncrementalDomainVersionProperties.Base());
    PartitionRemoteFileOps partitionRemoteFileOps = new LocalPartitionRemoteFileOps(localTmpDir, 0);
    long numRecords = getNumTotalRecords();
    int deltaFrequency = 1000000; // num records in delta = numRecords / deltaFrequency
    Cueball cueball = getCueball();
    // Create base
    Writer baseWriter = cueball.getWriter(
        baseVersion,
        partitionRemoteFileOps, 0);
    for (long i = 0; i < numRecords; ++i) {
      if (i % deltaFrequency != 0) {
        baseWriter.write(key(i, KEY_SIZE), value(i, VALUE_SIZE));
      }
    }
    baseWriter.close();
    // Create delta
    Writer deltaWriter = cueball.getWriter(
        deltaVersion,
        partitionRemoteFileOps, 0);
    for (long i = 0; i < numRecords; ++i) {
      if (i % deltaFrequency == 0) {
        deltaWriter.write(key(i, KEY_SIZE), value(i, VALUE_SIZE));
      }
    }
    deltaWriter.close();
    // Merge
    HankTimer timer = new HankTimer();
    new CueballMerger().merge(
        new CueballFilePath(localTmpDir + "/0/" + Cueball.getName(baseVersion)),
        Collections.singletonList(new CueballFilePath(localTmpDir + "/0/" + Cueball.getName(deltaVersion))),
        localTmpDir + "/" + Cueball.getName(newBaseVersion),
        KEY_HASH_SIZE,
        VALUE_SIZE,
        null,
        HASH_INDEX_BITS,
        new NoCompressionCodec());
    double elapsedS = timer.getDurationMs() / 1000.0;
    System.out.println("Merge done in " + elapsedS + " seconds");
  }

  private static ByteBuffer key(long key, int keySize) {
    byte[] keyBytes = new byte[keySize];
    EncodingHelper.encodeLittleEndianFixedWidthLong(key, keyBytes);
    return ByteBuffer.wrap(keyBytes);
  }

  private static ByteBuffer value(long value, int valueSize) {
    byte[] v = new byte[valueSize];
    Arrays.fill(v, (byte) value);
    return ByteBuffer.wrap(v);
  }

  // Hash function designed to read in longs and output hashes that conserve
  // the same ordering and satisfy the hash index
  private static class KeyHasher implements Hasher {

    private final int hashIndexBits;

    public KeyHasher(int hashIndexBits) {
      this.hashIndexBits = hashIndexBits;
    }

    @Override
    public void hash(ByteBuffer keyBytes, int keySize, byte[] hashBytes) {

      long key = EncodingHelper.decodeLittleEndianFixedWidthLong(keyBytes);

      if (hashIndexBits % 8 != 0) {
        throw new RuntimeException("hashIndexBits must be a multiple of 8");
      }

      long keyBlock = key / NUM_RECORDS_PER_BLOCK;
      long keyIndex = key % NUM_RECORDS_PER_BLOCK;
      byte[] keyHashBytes = new byte[keySize];

      // Note: this is valid because hashIndexBits must be a multiple of 8
      int hashIndexBytes = hashIndexBits / 8;

      // Encode bytes for key block
      EncodingHelper.encodeLittleEndianFixedWidthLong(keyBlock, keyHashBytes, keyHashBytes.length - hashIndexBytes, hashIndexBytes);

      // Encode bytes for key index in block
      EncodingHelper.encodeLittleEndianFixedWidthLong(keyIndex, keyHashBytes, 0, keyHashBytes.length - hashIndexBytes);

      for (int i = 0; i < keyHashBytes.length; ++i) {
        hashBytes[i] = (byte) (0xff & keyHashBytes[keyHashBytes.length - 1 - i]);
      }
    }
  }
}
