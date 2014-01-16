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
package com.liveramp.hank.storage.map;

import com.liveramp.commons.util.BytesUtils;
import com.liveramp.hank.storage.Writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class MapWriter implements Writer {
  public final Map<ByteBuffer, ByteBuffer> entries;
  private long recordsWritten;

  public MapWriter() {
    entries = new HashMap<ByteBuffer, ByteBuffer>();
  }

  public MapWriter(Map<ByteBuffer, ByteBuffer> keysAndValues) {
    this.entries = keysAndValues;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void write(ByteBuffer key, ByteBuffer value) throws IOException {
    recordsWritten++;
    if (entries.containsKey(key)) {
      throw new RuntimeException("Duplicate entry for key: " + BytesUtils.bytesToHexString(key));
    }
    ByteBuffer keyCopy = BytesUtils.byteBufferDeepCopy(key);
    ByteBuffer valueCopy = BytesUtils.byteBufferDeepCopy(value);
    entries.put(keyCopy, valueCopy);
  }

  @Override
  public long getNumBytesWritten() {
    // not really supported
    return 0;
  }

  @Override
  public long getNumRecordsWritten() {
    return recordsWritten;
  }
}
