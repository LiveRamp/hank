package com.liveramp.hank.compression.cueball;

import com.liveramp.hank.test.BaseTestCase;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class TestGzipCueballCompressionCodec extends BaseTestCase {
  private static final List<byte[]> TESTCASES = Arrays.asList(
      new byte[]{},
      new byte[]{0, 1, 2, 3, 4, 5, 6},
      lotsOfBytes(16 * 1024),
      lotsOfBytes(32 * 1024),
      lotsOfBytes(64 * 1024),
      lotsOfBytes(128 * 1024)
  );

  private static byte[] lotsOfBytes(int i) {
    Random r = new Random(i);
    byte[] bytes = new byte[i];
    int start = r.nextInt(255);
    for (int off = 0; off < i; off++) {
      bytes[off] = (byte) start;
      start = start + r.nextInt(127);
    }
    return bytes;
  }

  @Test
  public void testIt() throws Exception {
    GzipCueballCompressionCodec codec = new GzipCueballCompressionCodec();

    for (int i = 0; i < TESTCASES.size(); i++) {
      System.err.println("Testcase #" + i);
      byte[] testcase = TESTCASES.get(i);

      byte[] compressedBytes = new byte[codec.getMaxCompressBufferSize(testcase.length)];

      int compressedLength = codec.compress(testcase, 0, testcase.length, compressedBytes, 0);

      byte[] decompressedBytes = new byte[testcase.length + 1];
      int decompressedLength = codec.decompress(compressedBytes, 0, compressedLength, decompressedBytes, 0);
      assertEquals(testcase.length, decompressedLength);

      assertEquals(ByteBuffer.wrap(testcase), ByteBuffer.wrap(decompressedBytes, 0, decompressedLength));
    }
  }
}
