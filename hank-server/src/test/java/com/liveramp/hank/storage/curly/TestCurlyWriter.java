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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import com.liveramp.commons.util.BytesUtils;
import com.liveramp.hank.compression.CompressionCodec;
import com.liveramp.hank.storage.map.MapWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestCurlyWriter extends AbstractCurlyTestBase {
  @Test
  public void testWrite() throws Exception {
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    MapWriter keyfileWriter = new MapWriter();
    CurlyWriter writer = new CurlyWriter(s, keyfileWriter, 3, -1);

    writer.write(KEY1, VALUE1);
    writer.write(KEY2, VALUE2);
    writer.write(KEY3, VALUE3);
    assertTrue(writer.getNumBytesWritten() > 0);
    assertEquals(3, writer.getNumRecordsWritten());
    writer.close();

    // verify the keyfile looks as expected
    assertTrue(keyfileWriter.entries.containsKey(KEY1));
    assertEquals(ByteBuffer.wrap(new byte[]{0, 0, 0}), keyfileWriter.entries.get(KEY1));
    assertTrue(keyfileWriter.entries.containsKey(KEY2));
    assertEquals(ByteBuffer.wrap(new byte[]{5, 0, 0}), keyfileWriter.entries.get(KEY2));
    assertTrue(keyfileWriter.entries.containsKey(KEY3));
    assertEquals(ByteBuffer.wrap(new byte[]{10, 0, 0}), keyfileWriter.entries.get(KEY3));

    assertFalse(keyfileWriter.entries.containsKey(KEY4));

    // verify that the record stream looks as expected
    assertEquals(ByteBuffer.wrap(EXPECTED_RECORD_FILE), ByteBuffer.wrap(s.toByteArray()));
  }

  @Test
  public void testValueFolding() throws IOException {
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    MapWriter keyfileWriter = new MapWriter();
    CurlyWriter writer = new CurlyWriter(s, keyfileWriter, 3, 2);

    writer.write(KEY1, VALUE1);
    writer.write(KEY2, VALUE2);
    writer.write(KEY3, VALUE1);
    assertTrue(writer.getNumBytesWritten() > 0);
    assertEquals(3, writer.getNumRecordsWritten());
    writer.close();

    // verify the keyfile looks as expected
    assertTrue(keyfileWriter.entries.containsKey(KEY1));
    assertEquals(ByteBuffer.wrap(new byte[]{0, 0, 0}), keyfileWriter.entries.get(KEY1));
    assertTrue(keyfileWriter.entries.containsKey(KEY2));
    assertEquals(ByteBuffer.wrap(new byte[]{5, 0, 0}), keyfileWriter.entries.get(KEY2));
    assertTrue(keyfileWriter.entries.containsKey(KEY3));
    assertEquals(ByteBuffer.wrap(new byte[]{0, 0, 0}), keyfileWriter.entries.get(KEY3));

    assertFalse(keyfileWriter.entries.containsKey(KEY4));

    // verify that the record stream looks as expected
    assertEquals(ByteBuffer.wrap(EXPECTED_FOLDED_RECORD_FILE), ByteBuffer.wrap(s.toByteArray()));
  }

  private void doTestBlockCompression(CompressionCodec blockCompressionCodec, byte[] expectedBlock) throws IOException {
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    MapWriter keyfileWriter = new MapWriter();
    CurlyWriter writer = new CurlyWriter(s, keyfileWriter, 3, -1, blockCompressionCodec, 1024, 2);

    writer.write(KEY1, VALUE1);
    writer.write(KEY2, VALUE2);
    writer.write(KEY3, VALUE3);
    writer.close();
    assertTrue(writer.getNumBytesWritten() > 0);
    assertEquals(3, writer.getNumRecordsWritten());

    // verify the keyfile looks as expected
    assertTrue(keyfileWriter.entries.containsKey(KEY1));
    assertEquals(ByteBuffer.wrap(new byte[]{0, 0, 0, 0, 0}), keyfileWriter.entries.get(KEY1));
    assertTrue(keyfileWriter.entries.containsKey(KEY2));
    assertEquals(ByteBuffer.wrap(new byte[]{0, 0, 0, 5, 0}), keyfileWriter.entries.get(KEY2));
    assertTrue(keyfileWriter.entries.containsKey(KEY3));
    assertEquals(ByteBuffer.wrap(new byte[]{0, 0, 0, 10, 0}), keyfileWriter.entries.get(KEY3));

    assertFalse(keyfileWriter.entries.containsKey(KEY4));

    // verify that the record stream looks as expected
    System.out.println(BytesUtils.bytesToHexString(ByteBuffer.wrap(s.toByteArray())));
    assertEquals(ByteBuffer.wrap(expectedBlock), ByteBuffer.wrap(s.toByteArray()));
  }

  @Test
  public void testBlockCompressionSlowNoCompression() throws Exception {
    doTestBlockCompression(CompressionCodec.SLOW_NO_COMPRESSION, EXPECTED_RECORD_FILE_BLOCK_COMPRESSED_SLOW_NO_COMPRESSION);
  }

  @Test
  public void testBlockCompressionDeflate() throws Exception {
    doTestBlockCompression(CompressionCodec.DEFLATE, EXPECTED_RECORD_FILE_BLOCK_COMPRESSED_DEFLATE);
  }

  @Test
  public void testBlockCompressionGzip() throws Exception {
    doTestBlockCompression(CompressionCodec.GZIP, EXPECTED_RECORD_FILE_BLOCK_COMPRESSED_GZIP);
  }

  @Test
  public void testBlockCompressionSnappy() throws Exception {
    doTestBlockCompression(CompressionCodec.SNAPPY, EXPECTED_RECORD_FILE_BLOCK_COMPRESSED_SNAPPY);
  }
}
