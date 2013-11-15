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

import com.liveramp.hank.compression.cueball.NoCueballCompressionCodec;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TestCueballMerger extends AbstractCueballTest {
  private final String LOCAL_ROOT = localTmpDir;

  private final CueballFilePath DELTA_2 = new CueballFilePath(LOCAL_ROOT + "/00002.delta.cueball");

  private final CueballFilePath DELTA_1 = new CueballFilePath(LOCAL_ROOT + "/00001.delta.cueball");

  private final CueballFilePath BASE = new CueballFilePath(LOCAL_ROOT + "/00000.base.cueball");


  // base   k1 v1 k5 v5 | k10 v10
  // delta1 k1 v1 k2 v2 | k12 v12
  // delta2 k3 v3 k4 v4 | k11 v11


  private static final byte[] BASE_DATA = {
      // key 1
      0x01,// 2, 3, 4, 5,
      // value 1
      1, //4, 3, 2,
      // key 5
      0x05,// 6, 7, 8, 9,
      // value 5
      5, //8, 7, 6,
      // key 10
      (byte) 0x8a, //11, 12, 13, 14,
      // value 10
      10, //13, 12, 11,
      // block 0 offset
      0, 0, 0, 0, 0, 0, 0, 0,
      // block 1 offset
      2, 0, 0, 0, 0, 0, 0, 0,
      // max uncompressed size
      4, 0, 0, 0,
      // max compressed size
      4, 0, 0, 0,
  };

  private static final byte[] DELTA_1_DATA = {
      // key 1
      0x01, //2, 3, 4, 5,
      // value 1 - new version!
      2, //4, 3, 2,
      // key 2
      0x02, //3, 4, 5, 6,
      // value 2
      2, //5, 4, 3,
      // key 12
      (byte) 0x8c, //1, 2, 3, 4,
      // value 12
      12, //1, 2, 3,
      // block 0 offset
      0, 0, 0, 0, 0, 0, 0, 0,
      // block 1 offset
      4, 0, 0, 0, 0, 0, 0, 0,
      // max uncompressed size
      4, 0, 0, 0,
      // max compressed size
      4, 0, 0, 0,
  };

  private static final byte[] DELTA_2_DATA = {
      // key 3
      0x03, //4, 5, 6, 7,
      // value 3
      3, //6, 5, 4,
      // key 4
      0x04, //5, 6, 7, 8,
      // value 2
      4, //7, 6, 5,
      // key 11
      (byte) 0x8b, //12, 13, 14, 15,
      // value 11
      11, //14, 13, 12,
      // block 0 offset
      0, 0, 0, 0, 0, 0, 0, 0,
      // block 1 offset
      4, 0, 0, 0, 0, 0, 0, 0,
      // max uncompressed size
      4, 0, 0, 0,
      // max compressed size
      4, 0, 0, 0,
  };

  private final String NEW_BASE_PATH = LOCAL_ROOT + "/00002.base.cueball";

  private static final byte[] EXPECTED_MERGED_DATA = {
      // key 1
      0x01, //2, 3, 4, 5,
      // value 1
      2, //4, 3, 2,
      // key 2
      0x02, //3, 4, 5, 6,
      // value 2
      2, //5, 4, 3,
      // key 3
      0x03, //4, 5, 6, 7,
      // value 3
      3, //6, 5, 4,
      // key 4
      0x04, //5, 6, 7, 8,
      // value 4
      4, //7, 6, 5,
      // key 5
      0x05, //6, 7, 8, 9,
      // value 5
      5, //8, 7, 6,
      // key 10
      (byte) 0x8a, //11, 12, 13, 14,
      // value 10
      10, //13, 12, 11,
      // key 11
      (byte) 0x8b, //12, 13, 14, 15,
      // value 11
      11, //14, 13, 12,
      // key 12
      (byte) 0x8c, //1, 2, 3, 4,
      // value 12
      12, //1, 2, 3,
      // block 0 offset
      0, 0, 0, 0, 0, 0, 0, 0,
      // block 1 offset
      10, 0, 0, 0, 0, 0, 0, 0,
      // max uncompressed size
      10, 0, 0, 0,
      // max compressed size
      10, 0, 0, 0,
  };

  @Test
  public void testMerge() throws Exception {
    new File(LOCAL_ROOT).mkdirs();
    OutputStream s = new FileOutputStream(BASE.getPath());
    s.write(BASE_DATA);
    s.flush();
    s.close();

    s = new FileOutputStream(DELTA_1.getPath());
    s.write(DELTA_1_DATA);
    s.flush();
    s.close();

    s = new FileOutputStream(DELTA_2.getPath());
    s.write(DELTA_2_DATA);
    s.flush();
    s.close();

    new CueballMerger().merge(BASE,
        Arrays.asList(DELTA_1, DELTA_2),
        NEW_BASE_PATH,
        1,
        1,
        null,
        1,
        new NoCueballCompressionCodec());

    DataInputStream in = new DataInputStream(new FileInputStream(NEW_BASE_PATH));
    int length = (int) new File(NEW_BASE_PATH).length();
    byte[] actualMergedData = new byte[length];
    in.readFully(actualMergedData);

    assertEquals(ByteBuffer.wrap(EXPECTED_MERGED_DATA), ByteBuffer.wrap(actualMergedData));
  }
}
