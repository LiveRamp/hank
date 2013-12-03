package com.liveramp.hank.hasher;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestLeftPaddedIdentityHasher {

  @Test
  public void testPad() throws Exception {

    LeftPaddedIdentityHasher hasher = new LeftPaddedIdentityHasher();

    byte[] result = new byte[4];
    hasher.hash(ByteBuffer.wrap(new byte[]{1, 2}), 4, result);
    assertTrue(Arrays.equals(new byte[]{0,0,1,2}, result));

    byte[] result2 = new byte[4];
    hasher.hash(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}), 4, result2);
    assertTrue(Arrays.equals(new byte[]{1, 2, 3, 4}, result2));

    try{
      byte[] result3 = new byte[4];
      hasher.hash(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5}), 4, result3);
      fail();
    }catch(Exception e){
      //  cool
    }

  }
}
