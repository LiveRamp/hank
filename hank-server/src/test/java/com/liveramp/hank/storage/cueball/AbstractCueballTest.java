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
package com.liveramp.hank.storage.cueball;

import com.liveramp.hank.test.BaseTestCase;
import com.liveramp.hank.hasher.Hasher;
import com.liveramp.hank.test.hasher.MapHasher;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

public abstract class AbstractCueballTest extends BaseTestCase {
  protected static final byte[] EXPECTED_DATA = new byte[]{
      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // KEY1
      1, 2, 1, 2, 1, // value 1 (truncated)
      2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // KEY2
      2, 1, 2, 1, 2,// value 2 (truncated)
      (byte) 0x8f, (byte) 0x8f, (byte) 0x8f, (byte) 0x8f, (byte) 0x8f, (byte) 0x8f, (byte) 0x8f, (byte) 0x8f, (byte) 0x8f, (byte) 0x8f, // KEY3
      (byte) 0x8f, 1, 2, 1, 2,// value 3 (truncated)
      0, 0, 0, 0, 0, 0, 0, 0, // offset for hash prefix 0
      30, 0, 0, 0, 0, 0, 0, 0, // offset for hash prefix 1
      30, 0, 0, 0, // max uncompressed block
      30, 0, 0, 0, // max compressed block
  };
  protected static final byte[] KEY1 = new byte[]{1, 2, 3};
  protected static final byte[] VALUE1 = new byte[]{1, 2, 1, 2, 1, 2};
  protected static final byte[] KEY2 = new byte[]{4, 5, 6};
  protected static final byte[] VALUE2 = new byte[]{2, 1, 2, 1, 2, 1};

  protected static final byte[] KEY3 = new byte[]{(byte) 0x8f, 9, 9};
  protected static final byte[] KEY10 = new byte[]{(byte) 0xcf, 9, 9};
  protected static final byte[] KEY4 = new byte[]{10, 10, 10};

  protected static final Hasher HASHER = new MapHasher(new HashMap<ByteBuffer, byte[]>() {{
    byte[] hsh1 = new byte[10];
    Arrays.fill(hsh1, (byte) 1);
    put(ByteBuffer.wrap(KEY1), hsh1);

    byte[] hsh2 = new byte[10];
    Arrays.fill(hsh2, (byte) 2);
    put(ByteBuffer.wrap(KEY2), hsh2);

    byte[] hsh3 = new byte[10];
    Arrays.fill(hsh3, (byte) 0x8f);
    put(ByteBuffer.wrap(KEY3), hsh3);

    byte[] hsh4 = new byte[10];
    Arrays.fill(hsh4, (byte) 2);
    hsh4[0] = 1;
    put(ByteBuffer.wrap(KEY4), hsh4);

    byte[] hsh5 = new byte[10];
    Arrays.fill(hsh5, (byte) 0xcf);
    put(ByteBuffer.wrap(KEY10), hsh5);
  }});

  protected void set(byte[] hashBytes, int i) {
    for (int idx = 0; idx < hashBytes.length; idx++) {
      hashBytes[idx] = (byte) i;
    }
  }
}
