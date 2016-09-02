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
package com.liveramp.hank.util;

import java.nio.ByteBuffer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestEncodingHelper {
  @Test
  public void testEncodeLittleEndianFixedWidthLong() throws Exception {
    byte[] arr = new byte[3];
    EncodingHelper.encodeLittleEndianFixedWidthLong(1, arr);
    assertEquals(ByteBuffer.wrap(new byte[]{1, 0, 0}), ByteBuffer.wrap(arr));
    EncodingHelper.encodeLittleEndianFixedWidthLong(-1, arr);
    assertEquals(ByteBuffer.wrap(new byte[]{-1, -1, -1}), ByteBuffer.wrap(arr));
    EncodingHelper.encodeLittleEndianFixedWidthLong(0x001ff, arr);
    assertEquals(ByteBuffer.wrap(new byte[]{-1, 1, 0}), ByteBuffer.wrap(arr));
  }

  @Test
  public void testDecodeLittleEndianFixedWidthLong() throws Exception {
    byte[] arr = new byte[]{1, 0, 0};
    assertEquals(1, EncodingHelper.decodeLittleEndianFixedWidthLong(ByteBuffer.wrap(arr)));
    arr = new byte[]{-1, -1, -1};
    assertEquals(0xffffffL, EncodingHelper.decodeLittleEndianFixedWidthLong(ByteBuffer.wrap(arr)));
    arr = new byte[]{-1, 1, 0};
    assertEquals(0x0001ff, EncodingHelper.decodeLittleEndianFixedWidthLong(ByteBuffer.wrap(arr)));
  }

  @Test
  public void testEncodeLitteEndianVarInt() throws Exception {
    byte[] buffer = new byte[EncodingHelper.MAX_VARINT_SIZE];
    assertEquals(1, EncodingHelper.encodeLittleEndianVarInt(1, buffer));
    assertEquals(ByteBuffer.wrap(new byte[]{1}), ByteBuffer.wrap(buffer, 0, 1));

    assertEquals(1, EncodingHelper.encodeLittleEndianVarInt(10, buffer));
    assertEquals(ByteBuffer.wrap(new byte[]{10}), ByteBuffer.wrap(buffer, 0, 1));

    assertEquals(1, EncodingHelper.encodeLittleEndianVarInt(127, buffer));
    assertEquals(ByteBuffer.wrap(new byte[]{127}), ByteBuffer.wrap(buffer, 0, 1));

    assertEquals(2, EncodingHelper.encodeLittleEndianVarInt(128, buffer));
    assertEquals(ByteBuffer.wrap(new byte[]{(byte)0x80, 1}), ByteBuffer.wrap(buffer, 0, 2));

    assertEquals(5, EncodingHelper.encodeLittleEndianVarInt(0x7fffffff, buffer));
    assertEquals(ByteBuffer.wrap(new byte[]{-1, -1, -1, -1, 0x07}), ByteBuffer.wrap(buffer, 0, 5));
  }

  @Test
  public void testDecodeLittleEndianVarInt() throws Exception {
    assertEquals(1, EncodingHelper.decodeLittleEndianVarInt(ByteBuffer.wrap(new byte[]{1})));
    assertEquals(10, EncodingHelper.decodeLittleEndianVarInt(ByteBuffer.wrap(new byte[]{10})));
    assertEquals(127, EncodingHelper.decodeLittleEndianVarInt(ByteBuffer.wrap(new byte[]{127})));
    assertEquals(128, EncodingHelper.decodeLittleEndianVarInt(ByteBuffer.wrap(new byte[]{-128, 1})));
    assertEquals(10, EncodingHelper.decodeLittleEndianVarInt(ByteBuffer.wrap(new byte[]{10})));
    assertEquals(0x7fffffff, EncodingHelper.decodeLittleEndianVarInt(ByteBuffer.wrap(new byte[]{-1, -1, -1, -1, 0x07})));
  }
}
