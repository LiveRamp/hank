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
package com.rapleaf.hank.hasher;

import com.rapleaf.hank.util.Bytes;
import junit.framework.TestCase;

import java.nio.ByteBuffer;

public class TestMurmur64Hasher extends TestCase {

  private Murmur64Hasher hsh = new Murmur64Hasher();

  public void testIt() {
    byte[] bufA = new byte[9];
    hsh.hash(ByteBuffer.wrap(new byte[]{1, 2, 3}), bufA.length, bufA);
    byte[] bufB = new byte[9];
    hsh.hash(ByteBuffer.wrap(new byte[]{1, 2, 3}), bufB.length, bufB);

    assertTrue(bufA[8] != 0);
    assertEquals(ByteBuffer.wrap(bufA), ByteBuffer.wrap(bufB));
  }

  public void testCollision() {
    byte[] hashA = new byte[10];
    hsh.hash(ByteBuffer.wrap(new byte[]{0x28, 0x16, 0x70, 0x69, 0x63, 0x6b, 0x6c, 0x65, 0x73, 0x2e, 0x31, 0x39,
        0x39, 0x35, 0x40, 0x67, 0x6d, 0x61, 0x69, 0x6c, 0x2e, 0x63, 0x6f, 0x6d, 0x00}), hashA.length, hashA);
    byte[] hashB = new byte[10];
    hsh.hash(ByteBuffer.wrap(new byte[]{0x28, 0x16, 0x70, 0x69, 0x63, 0x6b, 0x6c, 0x65, 0x73, 0x71, 0x75, 0x69,
        0x6c, 0x74, 0x40, 0x67, 0x6d, 0x61, 0x69, 0x6c, 0x2e, 0x63, 0x6f, 0x6d, 0x00}), hashB.length, hashB);

    int comparision = Bytes.compareBytesUnsigned(hashA, 0, hashB, 0, 10);

    assertTrue(0 != comparision);
  }
}
