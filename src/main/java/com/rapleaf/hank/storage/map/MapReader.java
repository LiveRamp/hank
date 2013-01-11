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
package com.rapleaf.hank.storage.map;

import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.ReaderResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

public class MapReader implements Reader {

  private final Map<ByteBuffer, byte[]> map;
  private final Integer versionNumber;

  public MapReader(Integer versionNumber, byte[]... keysAndValues) {
    this.versionNumber = versionNumber;
    if (keysAndValues.length % 2 != 0) {
      throw new IllegalArgumentException("You must pass an even number of byte[]s to this constructor");
    }

    map = new TreeMap<ByteBuffer, byte[]>();

    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put(ByteBuffer.wrap(keysAndValues[i]), keysAndValues[i + 1]);
    }
  }

  @Override
  public void get(ByteBuffer key, ReaderResult result) throws IOException {
    byte[] v = map.get(key);
    if (v == null) {
      result.notFound();
    } else {
      result.found();
      result.requiresBufferSize(v.length);
      System.arraycopy(v, 0, result.getBuffer().array(), 0, v.length);
      result.getBuffer().limit(v.length);
      result.getBuffer().rewind();
    }
  }

  @Override
  public Integer getVersionNumber() {
    return versionNumber;
  }

  @Override
  public void close() throws IOException {
  }
}
