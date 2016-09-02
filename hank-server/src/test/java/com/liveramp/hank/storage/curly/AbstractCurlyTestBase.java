/**
 * Copyright 2011 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.liveramp.hank.storage.curly;

import java.nio.ByteBuffer;

import org.junit.Before;

public abstract class AbstractCurlyTestBase {

  protected static final ByteBuffer KEY1 = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});
  protected static final ByteBuffer VALUE1 = ByteBuffer.wrap(new byte[]{4, 3, 2, 1});
  protected static final ByteBuffer KEY2 = ByteBuffer.wrap(new byte[]{5, 6, 7, 8});
  protected static final ByteBuffer VALUE2 = ByteBuffer.wrap(new byte[]{8, 7, 6, 5});
  protected static final ByteBuffer KEY3 = ByteBuffer.wrap(new byte[]{9, 10, 11, 12});
  protected static final ByteBuffer VALUE3 = ByteBuffer.wrap(new byte[]{12, 11, 10, 9});

  protected static final ByteBuffer KEY4 = ByteBuffer.wrap(new byte[]{9, 9, 9, 9});

  protected static final byte[] EXPECTED_RECORD_FILE = new byte[]{
      4, 4, 3, 2, 1,
      4, 8, 7, 6, 5,
      4, 12, 11, 10, 9
  };

  protected static final byte[] EXPECTED_RECORD_FILE_BLOCK_COMPRESSED_SLOW_NO_COMPRESSION = new byte[]{
      15, // block size
      4, 4, 3, 2, 1,
      4, 8, 7, 6, 5,
      4, 12, 11, 10, 9
  };

  protected static final byte[] EXPECTED_RECORD_FILE_BLOCK_COMPRESSED_DEFLATE = new byte[]{
      23, // block size
      0x78, (byte)0xda, 0x63, 0x61, 0x61, 0x66, 0x62, 0x64, (byte)0xe1, 0x60, 0x67, 0x63,
      0x65, (byte)0xe1, (byte)0xe1, (byte)0xe6, (byte)0xe2, 0x04, 0x00, 0x02, 0x3f, 0x00, 0x5b
  };

  protected static final byte[] EXPECTED_RECORD_FILE_BLOCK_COMPRESSED_GZIP = new byte[]{
      35, // block size
      0x1f, (byte)0x8b, 0x08, 0, 0, 0, 0, 0, 0, 0, 0x63, 0x61, 0x61, 0x66, 0x62, 0x64,
      (byte)0xe1, 0x60, 0x67, 0x63, 0x65, (byte)0xe1, (byte)0xe1, (byte)0xe6,
      (byte)0xe2, 0x04, 0x00, 0x4b, (byte)0xcd, (byte)0xa7, 0x6f, 0x0f, 0, 0, 0
  };

  protected static final byte[] EXPECTED_RECORD_FILE_BLOCK_COMPRESSED_SNAPPY = new byte[]{
      49, // block size
      (byte)0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
      0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x07, 0x05, 0x10, 0x04, 0x04, 0x03, 0x02, 0x01,
      0x00, 0x00, 0x00, 0x07, 0x05, 0x10, 0x04, 0x08, 0x07, 0x06, 0x05, 0x00, 0x00, 0x00,
      0x07, 0x05, 0x10, 0x04, 0x0c, 0x0b, 0x0a, 0x09
  };

  protected static final byte[] EXPECTED_FOLDED_RECORD_FILE = new byte[]{
      4, 4, 3, 2, 1,
      4, 8, 7, 6, 5
  };

  @Before
  public void setUp() throws Exception {
    System.out.println("-----------------");
  }
}
