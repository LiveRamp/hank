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
package com.rapleaf.tiamat.storage.curly;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.rapleaf.tiamat.storage.Reader;
import com.rapleaf.tiamat.storage.Result;
import com.rapleaf.tiamat.util.EncodingHelper;

public class CurlyReader implements Reader {
  private final Reader keyfile;
  private final int readBufferSize;
  private final FileChannel recordFile;

  public CurlyReader(String partitionRoot,
      int recordFileReadBufferBytes, Reader keyfileReader) throws IOException
  {
    this.recordFile = new FileInputStream(Curly.getBases(partitionRoot).last()).getChannel();
    this.keyfile = keyfileReader;
    this.readBufferSize = recordFileReadBufferBytes;
  }

  @Override
  public void get(ByteBuffer key, Result result) throws IOException {
    result.requiresBufferSize(readBufferSize);

    keyfile.get(key, result);

    if (result.isFound()) {
      ByteBuffer buffer = result.getBuffer();
      long recordFileOffset = EncodingHelper.decodeLittleEndianFixedWidthLong(buffer);

      buffer.rewind();
      buffer.limit(readBufferSize);
      recordFile.read(buffer, recordFileOffset);
      buffer.rewind();
      int recordSize = EncodingHelper.decodeLittleEndianVarInt(buffer);
      int bytesInRecordSize = buffer.position();
      if (buffer.remaining() < recordSize) {
        int newSize = recordSize + EncodingHelper.MAX_VARINT_SIZE;
        result.requiresBufferSize(newSize);
        recordFile.read(buffer, recordFileOffset + bytesInRecordSize);
        buffer.position(0);
      }
      buffer.limit(recordSize + buffer.position());
    }
  }
}
