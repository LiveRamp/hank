package com.liveramp.hank.storage.cueball;

import com.liveramp.hank.test.BaseTestCase;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestFooter extends BaseTestCase {
  private final String filePath = localTmpDir + "/testfile";

  @Test
  public void testValid() throws Exception {
    final FileOutputStream out = new FileOutputStream(filePath);
    out.write(new byte[]{
        // not going to have any actual data section
        5,0,0,0,0,0,0,0,
        25,0,0,0,0,0,0,0,
        125,0,0,0,0,0,0,0,
        (byte) 0xff,0,0,0,0,0,0,0,
        (byte) 130,0,0,0,
        (byte) 250,0,0,0,
    });
    out.flush();
    out.close();

    final Footer footer = new Footer(new FileInputStream(filePath).getChannel(), 2);
    assertEquals(0, footer.getDataLength());
    assertEquals(40, footer.getFileSize());
    assertEquals(250, footer.getMaxCompressedBufferSize());
    assertEquals(130, footer.getMaxUncompressedBufferSize());
    assertTrue(Arrays.equals(new long[]{5, 25, 125, 255}, footer.getHashIndex()));
  }

  private static final List<byte[]> INVALID_CASES = Arrays.asList(
      // offset inversion
      new byte[]{
          // not going to have any actual data section
          25,0,0,0,0,0,0,0,
          5,0,0,0,0,0,0,0,
          125,0,0,0,0,0,0,0,
          (byte) 0xff,0,0,0,0,0,0,0,
          (byte) 130,0,0,0,
          (byte) 250,0,0,0,
      },
      // negative block offset
      new byte[]{
          // not going to have any actual data section
          5,0,0,0,0,0,0,(byte) 255,
          25,0,0,0,0,0,0,0,
          125,0,0,0,0,0,0,0,
          (byte) 0xff,0,0,0,0,0,0,0,
          (byte) 130,0,0,0,
          (byte) 250,0,0,0,
      },
      // negative buffer size
      new byte[]{
          // not going to have any actual data section
          5,0,0,0,0,0,0,0,
          25,0,0,0,0,0,0,0,
          125,0,0,0,0,0,0,0,
          (byte) 0xff,0,0,0,0,0,0,0,
          (byte) 130,0,0,(byte) 0x80,
          (byte) 250,0,0,0,
      },
      // negative buffer size
      new byte[]{
          // not going to have any actual data section
          5,0,0,0,0,0,0,0,
          25,0,0,0,0,0,0,0,
          125,0,0,0,0,0,0,0,
          (byte) 0xff,0,0,0,0,0,0,0,
          (byte) 130,0,0,0,
          (byte) 250,0,0,(byte) 0x80,
      }
  );

  @Test
  public void testInvalid() throws Exception {
    for (byte[] INVALID_CASE : INVALID_CASES) {
      final FileOutputStream out = new FileOutputStream(filePath);
      out.write(INVALID_CASE);
      out.flush();
      out.close();

      try {
        new Footer(new FileInputStream(filePath).getChannel(), 2);
        fail("should have thrown an exception");
      } catch (Exception e) {
      }
    }
  }

}
