package com.rapleaf.hank.storage.cueball;

import com.rapleaf.hank.compress.NoCompressionCodec;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;
import com.rapleaf.hank.hasher.Murmur64Hasher;
import com.rapleaf.hank.storage.LocalPartitionFileStreamFactory;
import com.rapleaf.hank.storage.LocalPartitionRemoteFileOps;
import com.rapleaf.hank.storage.PartitionFileStreamFactory;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.util.EncodingHelper;
import com.rapleaf.hank.util.FsUtils;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class PerformanceTestCueballWriter {
  private static final int VALUE_SIZE = 16;
  private static final int KEY_SIZE = 20;

  public static void main(String[] args) throws Exception {
    int numRecords = 3000000;
    String tmpDir = "/Users/bryanduxbury/hank_test_data";

    FsUtils.rmrf(tmpDir);
    new File(tmpDir).mkdirs();

    Cueball cueball = new Cueball(5,
        new Murmur64Hasher(), VALUE_SIZE, 6, "/tmp/remote_domains_root",
        new LocalPartitionRemoteFileOps.Factory(), NoCompressionCodec.class, null, 0, -1);
    PartitionFileStreamFactory localFs = new LocalPartitionFileStreamFactory(tmpDir);
    Writer writer = cueball.getWriter(new MockDomainVersion(1, 0L, null), localFs, 0);

    long start = System.currentTimeMillis();
    for (int i = 0; i < numRecords; i++) {
      writer.write(key(i, KEY_SIZE), value(i, VALUE_SIZE));
    }
    writer.close();
    long end = System.currentTimeMillis();
    long elapsedMs = end - start;

    System.out.println("Test took " + elapsedMs + "ms.");
    double elapsedSecs = elapsedMs / 1000.0;
    System.out.println(String.format("Throughput: %.2f writes/sec", numRecords / elapsedSecs));
    int totalBytes = numRecords * (KEY_SIZE + VALUE_SIZE);
    System.out.println(String.format("Throughput: %.2f MB/sec", totalBytes / 1024 / 1024 / elapsedSecs));
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
