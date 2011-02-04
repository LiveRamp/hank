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
package com.rapleaf.tiamat.storage.cueball;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.TreeSet;

public class TestCueballMerger extends AbstractCueballTest {
  private static final String LOCAL_ROOT = "/tmp/TestCueballMerger";

  private static final String DELTA_2_FILE_PATH = LOCAL_ROOT + "/00002.delta.cueball";

  private static final String DELTA_1_FILE_PATH = LOCAL_ROOT + "/00001.delta.cueball";

  private static final String BASE_FILE_PATH = LOCAL_ROOT + "/00000.base.cueball";

  private static final byte[] BASE_DATA = {
    // key 1
    1, 2, 3, 4, 5,
    // value 1
    5, 4, 3, 2,
    // key 5
    5, 6, 7, 8, 9, 
    // value 5
    9, 8, 7, 6,
    // key 10
    10, 11, 12, 13, 14,
    // value 10
    14, 13, 12, 11
  };
  
  private static final byte[] DELTA_1_DATA = {
    // key 1
    1, 2, 3, 4, 5,
    // value 1
    5, 4, 3, 2,
    // key 2
    2, 3, 4, 5, 6,
    // value 2
    6, 5, 4, 3
  };
  
  private static final byte[] DELTA_2_DATA = {
    // key 3
    3, 4, 5, 6, 7,
    // value 3
    7, 6, 5, 4,
    // key 4
    4, 5, 6, 7, 8,
    // value 2
    8, 7, 6, 5,
    // key 11
    11, 12, 13, 14, 15,
    // value 11
    15, 14, 13, 12
  };

  private static final String NEW_BASE_PATH = LOCAL_ROOT + "/00002.base.cueball";

  private static final byte[] EXPECTED_MERGED_DATA = {
    // key 1
    1, 2, 3, 4, 5,
    // value 1
    5, 4, 3, 2,
    // key 2
    2, 3, 4, 5, 6,
    // value 2
    6, 5, 4, 3,
    // key 3
    3, 4, 5, 6, 7,
    // value 3
    7, 6, 5, 4,
    // key 4
    4, 5, 6, 7, 8,
    // value 4
    8, 7, 6, 5,
    // key 5
    5, 6, 7, 8, 9, 
    // value 5
    9, 8, 7, 6,
    // key 10
    10, 11, 12, 13, 14,
    // value 10
    14, 13, 12, 11,
    // key 11
    11, 12, 13, 14, 15,
    // value 11
    15, 14, 13, 12
  };

  public void testMerge() throws Exception {
    new File(LOCAL_ROOT).mkdirs();
    OutputStream s = new FileOutputStream(BASE_FILE_PATH);
    s.write(BASE_DATA);
    s.flush();
    s.close();

    s = new FileOutputStream(DELTA_1_FILE_PATH);
    s.write(DELTA_1_DATA);
    s.flush();
    s.close();

    s = new FileOutputStream(DELTA_2_FILE_PATH);
    s.write(DELTA_2_DATA);
    s.flush();
    s.close();

    new CueballMerger().merge(BASE_FILE_PATH, new TreeSet<String>(Arrays.asList(DELTA_1_FILE_PATH, DELTA_2_FILE_PATH)), NEW_BASE_PATH, 5, 4, 32767, null);

    DataInputStream in = new DataInputStream(new FileInputStream(NEW_BASE_PATH));
    int length = (int) new File(NEW_BASE_PATH).length();
    byte[] actualMergedData = new byte[length];
    in.readFully(actualMergedData);

    assertEquals(ByteBuffer.wrap(EXPECTED_MERGED_DATA), ByteBuffer.wrap(actualMergedData));
  }
}
