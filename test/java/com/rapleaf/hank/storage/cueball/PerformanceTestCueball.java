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

import com.rapleaf.hank.compress.NoCompressionCodec;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;
import com.rapleaf.hank.hasher.Murmur64Hasher;
import com.rapleaf.hank.performance.HankTimer;
import com.rapleaf.hank.storage.LocalPartitionRemoteFileOps;
import com.rapleaf.hank.storage.PartitionRemoteFileOps;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.util.EncodingHelper;
import com.rapleaf.hank.util.FsUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class PerformanceTestCueball {

  private static final int VALUE_SIZE = 16;
  private static final int KEY_SIZE = 20;

  public static void performanceTestCueballWriter(String tmpDir) throws IOException {
    int numRecords = 3000000;

    FsUtils.rmrf(tmpDir);
    new File(tmpDir).mkdirs();

    Cueball cueball = new Cueball(5,
        new Murmur64Hasher(), VALUE_SIZE, 6, tmpDir + "/remote_domain_root",
        new LocalPartitionRemoteFileOps.Factory(), NoCompressionCodec.class, null, 0, -1);
    PartitionRemoteFileOps localFs = new LocalPartitionRemoteFileOps(tmpDir, 0);
    Writer writer = cueball.getWriter(new MockDomainVersion(1, 0L, null), localFs, 0);

    HankTimer timer = new HankTimer();
    for (int i = 0; i < numRecords; i++) {
      writer.write(key(i, KEY_SIZE), value(i, VALUE_SIZE));
    }
    writer.close();
    double elapsedMs = timer.getDuration() / 1000000.0;

    System.out.println("Test took " + elapsedMs + "ms.");
    double elapsedSecs = elapsedMs / 1000.0;
    System.out.println(String.format("Throughput: %.2f writes/sec", numRecords / elapsedSecs));
    int totalBytes = numRecords * (KEY_SIZE + VALUE_SIZE);
    System.out.println(String.format("Throughput: %.2f MB/sec", totalBytes / 1024 / 1024 / elapsedSecs));
  }

  public static void main(String[] args) throws IOException {
    performanceTestCueballWriter(args[0]);
  }

  private static ByteBuffer key(int keyNum, int len) {
    byte[] k = new byte[len];
    EncodingHelper.encodeLittleEndianFixedWidthLong(keyNum, k);
    return ByteBuffer.wrap(k);
  }

  private static ByteBuffer value(int i, int j) {
    byte[] v = new byte[j];
    Arrays.fill(v, (byte) i);
    return ByteBuffer.wrap(v);
  }
}
