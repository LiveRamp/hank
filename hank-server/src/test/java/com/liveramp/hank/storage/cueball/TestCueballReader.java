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

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.junit.Test;

import com.liveramp.hank.compression.cueball.NoCueballCompressionCodec;
import com.liveramp.hank.storage.ReaderResult;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestCueballReader extends AbstractCueballTest {
  @Test
  public void testRead() throws Exception {
    // set up fake cueball file
    String root = localTmpDir + "/1";
    new File(root).mkdir();
    OutputStream os = new FileOutputStream(root + "/00000.base.cueball");
    os.write(EXPECTED_DATA);
    os.flush();
    os.close();

    CueballReader reader = new CueballReader(root, 10, HASHER, 5, 1, new NoCueballCompressionCodec(), 1 << 20, 1);

    // test version number
    assertEquals(Integer.valueOf(0), reader.getVersionNumber());

    ReaderResult result = new ReaderResult();
    reader.get(ByteBuffer.wrap(KEY1), result);
    assertTrue(result.isFound());
    assertEquals(ByteBuffer.wrap(new byte[]{1, 2, 1, 2, 1}), result.getBuffer());
    result.clear();

    reader.get(ByteBuffer.wrap(KEY3), result);
    assertTrue(result.isFound());
    assertEquals(ByteBuffer.wrap(new byte[]{(byte)0x8f, 1, 2, 1, 2}), result.getBuffer());
    result.clear();

    reader.get(ByteBuffer.wrap(KEY1), result);
    assertTrue(result.isFound());
    assertEquals(ByteBuffer.wrap(new byte[]{1, 2, 1, 2, 1}), result.getBuffer());
    result.clear();

    // non-existent key in occupied bucket 10
    reader.get(ByteBuffer.wrap(KEY4), result);
    assertFalse(result.isFound());
    result.clear();

    reader.get(ByteBuffer.wrap(KEY2), result);
    assertTrue(result.isFound());
    assertEquals(ByteBuffer.wrap(new byte[]{2, 1, 2, 1, 2}), result.getBuffer());
    result.clear();

    reader.get(ByteBuffer.wrap(KEY10), result);
    assertFalse(result.isFound());
    result.clear();

    // Test cache
    reader.get(ByteBuffer.wrap(KEY2), result);
    assertTrue(result.isFound());
    result.clear();

    reader.get(ByteBuffer.wrap(KEY1), result);
    assertTrue(result.isFound());
    assertEquals(ByteBuffer.wrap(new byte[]{1, 2, 1, 2, 1}), result.getBuffer());
    assertEquals(false, result.getL1CacheHit());
    assertEquals(false, result.getL2CacheHit());
    result.clear();

    reader.get(ByteBuffer.wrap(KEY1), result);
    assertTrue(result.isFound());
    assertEquals(ByteBuffer.wrap(new byte[]{1, 2, 1, 2, 1}), result.getBuffer());
    assertEquals(true, result.getL1CacheHit());
    assertEquals(false, result.getL2CacheHit());
    result.clear();

    // Test cache for keys not found
    reader.get(ByteBuffer.wrap(KEY4), result);
    assertFalse(result.isFound());
    assertEquals(false, result.getL1CacheHit());
    assertEquals(false, result.getL2CacheHit());
    result.clear();

    reader.get(ByteBuffer.wrap(KEY4), result);
    assertFalse(result.isFound());
    assertEquals(true, result.getL1CacheHit());
    assertEquals(false, result.getL2CacheHit());
    result.clear();
  }
}
