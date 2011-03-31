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

import junit.framework.TestCase;

public class TestBytes extends TestCase {
  private static final byte[] A = {1, 2, 3};

  private static final byte[] B = {1, 1, 3};
  private static final byte[] C = {1, 3, 3};
  private static final byte[] D = {1, 2, 4};
  private static final byte[] E = {(byte) 0x80, 2, 4};

  public void testCompareBytes() throws Exception {
    assertEquals(-1, Bytes.compareBytes(B, 0, A, 0, 3));
    assertEquals(1, Bytes.compareBytes(A, 0, B, 0, 3));
    assertEquals(-1, Bytes.compareBytes(A, 0, C, 0, 3));
    assertEquals(-1, Bytes.compareBytes(A, 1, C, 1, 2));
    assertEquals(0, Bytes.compareBytes(A, 1, D, 1, 1));

    assertEquals(-1, Bytes.compareBytes(A, 0, E, 0, 3));
    assertEquals(1, Bytes.compareBytes(E, 0, A, 0, 3));
  }
}
