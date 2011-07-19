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
package com.rapleaf.hank.storage.curly;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.rapleaf.hank.storage.Result;
import com.rapleaf.hank.storage.map.MapReader;

public class TestCurlyReader extends AbstractCurlyTestBase {
  private static final String TMP_TEST_CURLY_READER = "/tmp/TestCurlyReader";
  private static final ByteBuffer KEY5 = ByteBuffer.wrap(new byte[]{5, 5, 5, 5});

  private static final byte[] TWENTYK_BLOB;
  static {
    TWENTYK_BLOB = new byte[20*1024];
    for (int i = 0; i < TWENTYK_BLOB.length; i++) {
      TWENTYK_BLOB[i] = (byte) i;
    }
  }

  public void testReader() throws Exception {
    new File(TMP_TEST_CURLY_READER).mkdirs();
    OutputStream s = new FileOutputStream(TMP_TEST_CURLY_READER + "/00000.base.curly");
    s.write(EXPECTED_RECORD_FILE);
    s.write(new byte[] { (byte) 0x80, (byte) 0xa0, 1 });
    s.write(TWENTYK_BLOB);

    s.flush();
    s.close();

    MapReader keyfileReader = new MapReader(
        KEY1.array(), new byte[]{0, 0, 0},
        KEY2.array(), new byte[]{5, 0, 0},
        KEY3.array(), new byte[]{10, 0, 0},
        KEY5.array(), new byte[]{15, 0, 0}
    );

    CurlyReader reader = new CurlyReader(TMP_TEST_CURLY_READER, 1024, keyfileReader);

    Result result = new Result();

    reader.get(KEY1, result);
    assertTrue(result.isFound());
    assertEquals(VALUE1, result.getBuffer());

    reader.get(KEY4, result);
    assertFalse(result.isFound());

    reader.get(KEY3, result);
    assertTrue(result.isFound());
    assertEquals(VALUE3, result.getBuffer());

    reader.get(KEY2, result);
    assertTrue(result.isFound());
    assertEquals(VALUE2, result.getBuffer());

    reader.get(KEY5, result);
    assertTrue(result.isFound());
    assertEquals(20 * 1024, result.getBuffer().remaining());
    assertEquals(ByteBuffer.wrap(TWENTYK_BLOB), result.getBuffer());
  }
}
