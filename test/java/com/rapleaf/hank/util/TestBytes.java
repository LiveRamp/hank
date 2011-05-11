/**
 *  Copyright 2011 Rapleaf
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
package com.rapleaf.hank.util;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

public class TestBytes extends TestCase {
  private static final byte[] A = {1, 2, 3};

  private static final byte[] B = {1, 1, 3};
  private static final byte[] C = {1, 3, 3};
  private static final byte[] D = {1, 2, 4};
  private static final byte[] E = {(byte) 0x80, 2, 4};

  public void testCompareBytes() throws Exception {
    assertEquals(-1, Bytes.compareBytesUnsigned(B, 0, A, 0, 3));
    assertEquals(1, Bytes.compareBytesUnsigned(A, 0, B, 0, 3));
    assertEquals(-1, Bytes.compareBytesUnsigned(A, 0, C, 0, 3));
    assertEquals(-1, Bytes.compareBytesUnsigned(A, 1, C, 1, 2));
    assertEquals(0, Bytes.compareBytesUnsigned(A, 1, D, 1, 1));

    assertEquals(-1, Bytes.compareBytesUnsigned(A, 0, E, 0, 3));
    assertEquals(1, Bytes.compareBytesUnsigned(E, 0, A, 0, 3));
  }

  public void testException() {
    try {
      Bytes.compareBytesUnsigned(A, 1, B, 0, 3);
      fail("Should fail with an exception");
    } catch (Exception e) {
    }
    try {
      Bytes.compareBytesUnsigned(A, 0, B, 1, 3);
      fail("Should fail with an exception");
    } catch (Exception e) {
    }
  }

  public void testDeepCopy() {
    // Without allocation
    ByteBuffer copyA = Bytes.byteBufferDeepCopy(ByteBuffer.wrap(A));
    assertEquals(0, Bytes.compareBytesUnsigned(copyA, ByteBuffer.wrap(A)));

    // With allocation capable
    ByteBuffer copyB = null;

    byte[] v1 = {1};
    copyB = Bytes.byteBufferDeepCopy(ByteBuffer.wrap(v1), copyB);
    assertEquals(0, Bytes.compareBytesUnsigned(ByteBuffer.wrap(v1), copyB));

    byte[] v2 = {1, 2 , 3};
    copyB = Bytes.byteBufferDeepCopy(ByteBuffer.wrap(v2), copyB);
    assertEquals(0, Bytes.compareBytesUnsigned(ByteBuffer.wrap(v2), copyB));
  }
}
