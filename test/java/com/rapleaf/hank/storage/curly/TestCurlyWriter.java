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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import com.rapleaf.hank.storage.MapWriter;

public class TestCurlyWriter extends AbstractCurlyTestBase {
  public void testWrite() throws Exception {
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    MapWriter keyfileWriter = new MapWriter();
    CurlyWriter writer = new CurlyWriter(s, keyfileWriter, 3);

    writer.write(KEY1, VALUE1);
    writer.write(KEY2, VALUE2);
    writer.write(KEY3, VALUE3);
    writer.close();

    // verify the keyfile looks as expected
    assertTrue(keyfileWriter.map.containsKey(KEY1));
    assertEquals(ByteBuffer.wrap(new byte[]{0, 0, 0}), keyfileWriter.map.get(KEY1));
    assertTrue(keyfileWriter.map.containsKey(KEY2));
    assertEquals(ByteBuffer.wrap(new byte[]{5, 0, 0}), keyfileWriter.map.get(KEY2));
    assertTrue(keyfileWriter.map.containsKey(KEY3));
    assertEquals(ByteBuffer.wrap(new byte[]{10, 0, 0}), keyfileWriter.map.get(KEY3));

    assertFalse(keyfileWriter.map.containsKey(KEY4));

    // verify that the record stream looks as expected
    assertEquals(ByteBuffer.wrap(EXPECTED_RECORD_FILE), ByteBuffer.wrap(s.toByteArray()));
  }
}
