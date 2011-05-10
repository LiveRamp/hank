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
package com.rapleaf.hank.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public final class Bytes {
  private static final String CHARSET = "utf-8";

  private Bytes() {}

  public static int compareBytesUnsigned(byte[] a, int aOff, byte[] b, int bOff, int len) {
    if (len > a.length - aOff || len > b.length - bOff) {
      throw new RuntimeException("Not enough bytes left to compare!");
    }
    for (int i = 0; i < len; i++) {
      // we want our comparison to be unsigned. if we just compare the bytes,
      // it will be a signed comparison. to drop the sign, we convert the byte
      // to an int, then mask off all the upper bits. if we don't do the
      // masking, then the signed byte will just get sign-extended and remain
      // negative.
      final int ab = a[aOff + i] & 0xff;
      final int bb = b[bOff + i] & 0xff;
      if (ab > bb) {
        return 1;
      } else if (ab < bb) {
        return -1;
      }
    }
    return 0;
  }

  public static int compareBytesUnsigned(ByteBuffer a, ByteBuffer b) {
    if (a.remaining() != b.remaining()) {
      throw new RuntimeException("Cannot compare ByteBuffers that have a different number of remaining elements.");
    }
    return compareBytesUnsigned(a.array(), a.arrayOffset() + a.position(),
        b.array(), b.arrayOffset() + b.position(),
        a.remaining());
  }

  public static byte[] intToBytes(int value) {
    return new byte[] {
        (byte)(value >>> 24),
        (byte)(value >>> 16),
        (byte)(value >>> 8),
        (byte)value};
  }

  public static int bytesToInt(byte[] b) {
    return (b[0] << 24)
    + ((b[1] & 0xFF) << 16)
    + ((b[2] & 0xFF) << 8)
    + (b[3] & 0xFF);
  }

  public static String bytesToString(byte[] b) {
    try {
      return new String(b, CHARSET);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] stringToBytes(String s) {
    try {
      return s.getBytes(CHARSET);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static ByteBuffer byteBufferDeepCopy(ByteBuffer src) {
    src.mark();
    ByteBuffer copy = ByteBuffer.allocate(src.capacity()).put(src);
    copy.flip();
    src.reset();
    return copy;
  }

  public static String bytesToHexString(ByteBuffer b) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < b.remaining(); ++i) {
      final int v = b.array()[b.arrayOffset() + b.position() + i] & 0xff;
      if (i > 0) {
        result.append(" ");
      }
      result.append(Integer.toString(v, 16));
    }
    return result.toString();
  }
}
