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

public final class EncodingHelper {
  public static final int MAX_VARINT_SIZE = 5;

  private EncodingHelper() {
  }

  public static void encodeLittleEndianFixedWidthLong(long l, byte[] array) {
    encodeLittleEndianFixedWidthLong(l, array, 0, array.length);
  }

  public static void encodeLittleEndianFixedWidthLong(long value, byte[] array, int off, int len) {
    long l = value;
    for (int i = off; i < off + len; i++) {
      array[i] = (byte) (l & 0xff);
      l >>= 8;
    }
    // We should have wrapped around and filled l with the sign bit
    if (l != 0 && l != -1) {
      throw new RuntimeException("Supplied buffer (size=" + len + ") is not large enough to encode supplied value: " + value);
    }
  }

  public static long decodeLittleEndianFixedWidthLong(ByteBuffer buffer) {
    return decodeLittleEndianFixedWidthLong(buffer.array(),
        buffer.arrayOffset() + buffer.position(),
        buffer.remaining());
  }

  public static long decodeLittleEndianFixedWidthLong(byte[] bytes, int off, int len) {
    long result = 0;
    for (int i = off + len - 1; i >= off; i--) {
      result <<= 8;
      result |= bytes[i] & 0xff;
    }
    return result;
  }

  public static int encodeLittleEndianVarInt(int n, byte[] buffer) {
    if (n == 0) {
      buffer[0] = 0;
      return 1;
    }
    int i = 0;
    while (n > 0) {
      byte b = (byte) (n & 0x7f);
      n >>= 7;
      if (n > 0) {
        b |= 0x80;
      }
      buffer[i++] = b;
    }
    return i;
  }

  public static int decodeLittleEndianVarInt(ByteBuffer buffer) {
    byte b = buffer.get();
    int result = b & 0x7f;
    if ((b & 0x80) > 0) {
      b = buffer.get();
      result |= ((b & 0x7f) << 7);
      if ((b & 0x80) > 0) {
        b = buffer.get();
        result |= ((b & 0x7f) << 14);
        if ((b & 0x80) > 0) {
          b = buffer.get();
          result |= ((b & 0x7f) << 21);
          if ((b & 0x80) > 0) {
            b = buffer.get();
            result |= ((b & 0x07) << 28);
          }
        }
      }
    }
    return result;
  }
}
