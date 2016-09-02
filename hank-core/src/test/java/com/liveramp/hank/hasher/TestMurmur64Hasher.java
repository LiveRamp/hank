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
package com.liveramp.hank.hasher;

import com.liveramp.commons.util.BytesUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMurmur64Hasher {
  private Murmur64Hasher hsh = new Murmur64Hasher();

  @Test
  public void testIt() {
    byte[] bufA = new byte[9];
    hsh.hash(ByteBuffer.wrap(new byte[]{1, 2, 3}), bufA.length, bufA);
    byte[] bufB = new byte[9];
    hsh.hash(ByteBuffer.wrap(new byte[]{1, 2, 3}), bufB.length, bufB);

    assertTrue(bufA[8] != 0);
    assertEquals(ByteBuffer.wrap(bufA), ByteBuffer.wrap(bufB));
  }

  @Test
  public void testCollision() {
    byte[] hashA = new byte[10];
    hsh.hash(ByteBuffer.wrap(new byte[]{0x28, 0x16, 0x70, 0x69, 0x63, 0x6b, 0x6c, 0x65, 0x73, 0x2e, 0x31, 0x39,
        0x39, 0x35, 0x40, 0x67, 0x6d, 0x61, 0x69, 0x6c, 0x2e, 0x63, 0x6f, 0x6d, 0x00}), hashA.length, hashA);
    byte[] hashB = new byte[10];
    hsh.hash(ByteBuffer.wrap(new byte[]{0x28, 0x16, 0x70, 0x69, 0x63, 0x6b, 0x6c, 0x65, 0x73, 0x71, 0x75, 0x69,
        0x6c, 0x74, 0x40, 0x67, 0x6d, 0x61, 0x69, 0x6c, 0x2e, 0x63, 0x6f, 0x6d, 0x00}), hashB.length, hashB);

    int comparision = BytesUtils.compareBytesUnsigned(hashA, 0, hashB, 0, 10);

    assertTrue(0 != comparision);
  }

  @Test
  public void testMultipleOf8() {
    byte[] hash64 = new byte[8];
    byte[] hash128 = new byte[16];

    hsh.hash(ByteBuffer.wrap(new byte[]{0x1, 0x2, 0x3, 0x4}), 8, hash64);
    hsh.hash(ByteBuffer.wrap(new byte[]{0x1, 0x2, 0x3, 0x4}), 16, hash128);

    assertTrue(Arrays.equals(new byte[]{0x08, 0x51, 0x77, 0x72, 0x42, 0x6e, 0x7f, (byte) 0xa6}, hash64));
    assertTrue(Arrays.equals(new byte[]{0x08, 0x51, 0x77, 0x72, 0x42, 0x6e, 0x7f, (byte) 0xa6,
        (byte) 0x9f, (byte) 0xa8, 0x0d, 0x49, (byte) 0xde, 0x0a, (byte) 0x90, 0x15}, hash128));
  }

  @Test
  public void testNonZeroOffset() {
    byte[] hash64 = new byte[8];

    ByteBuffer buffer = ByteBuffer.wrap(new byte[]{0x0, 0x1, 0x2, 0x3, 0x4});
    buffer.get();
    assertEquals(1, buffer.position());
    assertEquals(4, buffer.remaining());
    hsh.hash(buffer, 8, hash64);
    assertTrue(Arrays.equals(new byte[]{0x08, 0x51, 0x77, 0x72, 0x42, 0x6e, 0x7f, (byte) 0xa6}, hash64));
  }
}
