package com.rapleaf.hank.storage.cueball;

import com.rapleaf.hank.BaseTestCase;

public class TestHashPrefixCalculator extends BaseTestCase {
  public void testLessThanEightBits() throws Exception {
    HashIndexPrefixCalculator c = new HashIndexPrefixCalculator(2);

    byte[] bytes = new byte[]{0, 0x3f, 0x40, (byte) 0x80, (byte) 0xcf};

    assertEquals(0, c.getHashPrefix(bytes, 0));
    assertEquals(0, c.getHashPrefix(bytes, 1));
    assertEquals(1, c.getHashPrefix(bytes, 2));
    assertEquals(2, c.getHashPrefix(bytes, 3));
    assertEquals(3, c.getHashPrefix(bytes, 4));
  }

  public void testAtLeastEightBits() throws Exception {
    HashIndexPrefixCalculator c = new HashIndexPrefixCalculator(10);

    byte[] bytes = new byte[]{0, 0x3f, 0x40, (byte) 0x80, (byte) 0xcf};

    assertEquals(0, c.getHashPrefix(bytes, 0));
    assertEquals(0x3f40 >> 6, c.getHashPrefix(bytes, 1));
    assertEquals(0x4080 >> 6, c.getHashPrefix(bytes, 2));
    assertEquals(0x80cf >> 6, c.getHashPrefix(bytes, 3));
  }
}
