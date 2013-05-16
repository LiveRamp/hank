/**
 *  Copyright 2013 LiveRamp
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

package com.liveramp.hank.compression.deflate;

import com.liveramp.hank.compression.Decompressor;
import com.liveramp.hank.util.IOStreamUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class DeflateDecompressor implements Decompressor {

  private final Inflater inflater = new Inflater();
  private final byte[] copyBuffer = new byte[IOStreamUtils.DEFAULT_BUFFER_SIZE];

  @Override
  public void decompressBlock(byte[] buffer, int offset, int length, OutputStream outputStream) throws IOException {
    inflater.reset();
    inflater.setInput(buffer, offset, length);
    while (true) {
      int numBytes;
      try {
        numBytes = inflater.inflate(copyBuffer);
      } catch (DataFormatException e) {
        throw new IOException(e);
      }
      if (numBytes > 0) {
        outputStream.write(copyBuffer, 0, numBytes);
      }
      if (inflater.finished()) {
        break;
      }
    }
  }
}
