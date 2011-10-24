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
package com.rapleaf.hank.storage.cueball;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.rapleaf.hank.compress.NoCompressionCodec;
import com.rapleaf.hank.storage.ReaderResult;

public class TestCueballReader extends AbstractCueballTest {
  public void testRead() throws Exception {
    // set up fake cueball file
    String root = localTmpDir + "/1";
    new File(root).mkdir();
    OutputStream os = new FileOutputStream(root + "/00000.base.cueball");
    os.write(EXPECTED_DATA);
    os.flush();
    os.close();

    CueballReader reader = new CueballReader(root, 10, HASHER, 5, 1, new NoCompressionCodec());

    // test version number
    assertEquals(Integer.valueOf(0), reader.getVersionNumber());

    ReaderResult result = new ReaderResult();
    reader.get(ByteBuffer.wrap(KEY1), result);
    assertTrue(result.isFound());
    assertEquals(ByteBuffer.wrap(new byte[]{1, 2, 1, 2, 1}), result.getBuffer());

    reader.get(ByteBuffer.wrap(KEY3), result);
    assertTrue(result.isFound());
    assertEquals(ByteBuffer.wrap(new byte[]{(byte) 0x8f, 1, 2, 1, 2}), result.getBuffer());

    reader.get(ByteBuffer.wrap(KEY1), result);
    assertTrue(result.isFound());
    assertEquals(ByteBuffer.wrap(new byte[]{1, 2, 1, 2, 1}), result.getBuffer());

    // non-existent key in occupied bucket 10
    reader.get(ByteBuffer.wrap(KEY4), result);
    assertFalse(result.isFound());

    reader.get(ByteBuffer.wrap(KEY2), result);
    assertTrue(result.isFound());
    assertEquals(ByteBuffer.wrap(new byte[]{2, 1, 2, 1, 2}), result.getBuffer());

    reader.get(ByteBuffer.wrap(KEY10), result);
    assertFalse(result.isFound());
  }
}
