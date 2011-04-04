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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.util.EncodingHelper;

public class CurlyWriter implements Writer {

  private final Writer keyfileWriter;
  private final OutputStream recordFileStream;

  private long currentRecordOffset;
  private final long maxOffset;
  private final ByteBuffer offsetBuffer;
  private final byte[] lengthBuffer = new byte[5];;

  public CurlyWriter(OutputStream recordfileStream,
      Writer keyfileWriter,
      int offsetSize)
  {
    this.recordFileStream = recordfileStream;
    this.keyfileWriter = keyfileWriter;
    this.maxOffset = 1L << (offsetSize * 8);
    this.currentRecordOffset = 0;

    offsetBuffer = ByteBuffer.wrap(new byte[offsetSize]);
  }

  @Override
  public void close() throws IOException {
    recordFileStream.flush();
    recordFileStream.close();
    keyfileWriter.close();
  }

  @Override
  public void write(ByteBuffer key, ByteBuffer value) throws IOException {
    if (currentRecordOffset > maxOffset) {
      throw new IOException("Exceeded configured max recordfile size of "
          + maxOffset
          + ". Increase number of partitions to go back below this level.");
    }

    EncodingHelper.encodeLittleEndianFixedWidthLong(currentRecordOffset, offsetBuffer.array());
    keyfileWriter.write(key, offsetBuffer);

    int valueLen = value.remaining();
    int numBytes = EncodingHelper.encodeLittleEndianVarInt(valueLen, lengthBuffer);
    recordFileStream.write(lengthBuffer, 0, numBytes);
    recordFileStream.write(value.array(), value.arrayOffset() + value.position(), valueLen);

    currentRecordOffset += numBytes + valueLen;
  }
}
