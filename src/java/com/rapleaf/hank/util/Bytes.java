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

public final class Bytes {
  private static final String CHARSET = "utf-8";

  private Bytes() {}

  public static int compareBytes(byte[] a, int aOff, byte[] b, int bOff, int len) {
    for (int i = 0; i < len; i++) {
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
}
