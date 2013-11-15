package com.liveramp.hank.storage.cueball;

import com.liveramp.hank.test.BaseTestCase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestHashPrefixCalculator extends BaseTestCase {

  @Test
  public void testLessThanEightBits() throws Exception {
    HashPrefixCalculator c = new HashPrefixCalculator(2);

    byte[] bytes = new byte[]{0, 0x3f, 0x40, (byte) 0x80, (byte) 0xcf};

    assertEquals(0, c.getHashPrefix(bytes, 0));
    assertEquals(0, c.getHashPrefix(bytes, 1));
    assertEquals(1, c.getHashPrefix(bytes, 2));
    assertEquals(2, c.getHashPrefix(bytes, 3));
    assertEquals(3, c.getHashPrefix(bytes, 4));
  }

  @Test
  public void testAtLeastEightBits() throws Exception {
    HashPrefixCalculator c = new HashPrefixCalculator(10);

    byte[] bytes = new byte[]{0, 0x3f, 0x40, (byte) 0x80, (byte) 0xcf};

    assertEquals(0, c.getHashPrefix(bytes, 0));
    assertEquals(0x3f40 >> 6, c.getHashPrefix(bytes, 1));
    assertEquals(0x4080 >> 6, c.getHashPrefix(bytes, 2));
    assertEquals(0x80cf >> 6, c.getHashPrefix(bytes, 3));
  }

  @Test
  public void testExactlyEightBits() throws Exception {
    HashPrefixCalculator c = new HashPrefixCalculator(8);

    byte[] bytes = new byte[]{0, 0x3f, 0x40, (byte) 0x80, (byte) 0xcf};

    assertEquals(0, c.getHashPrefix(bytes, 0));
    assertEquals(0x3f, c.getHashPrefix(bytes, 1));
    assertEquals(0x40, c.getHashPrefix(bytes, 2));
    assertEquals(0x80, c.getHashPrefix(bytes, 3));
    assertEquals(0xcf, c.getHashPrefix(bytes, 4));
  }
}
