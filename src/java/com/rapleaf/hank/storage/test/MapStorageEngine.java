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

package com.rapleaf.hank.storage.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.rapleaf.hank.storage.MapWriter;
import com.rapleaf.hank.storage.MockStorageEngine;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.Writer;

// Storage engine used for testing. Stores key-value pairs in a static
// partition map. It is not thread safe.
public class MapStorageEngine extends MockStorageEngine {

  private static Map<Integer, Map<ByteBuffer, ByteBuffer>> partitions;

  public MapStorageEngine() {
    this.partitions = new HashMap<Integer, Map<ByteBuffer, ByteBuffer>>();
  }

  public static Map<Integer, Map<ByteBuffer, ByteBuffer>> getPartitions() {
    return partitions;
  }

  @Override
  public Writer getWriter(OutputStreamFactory streamFactory, int partNum,
      int versionNumber, boolean base) throws IOException {
    if (!partitions.containsKey(partNum)) {
      partitions.put(partNum, new HashMap<ByteBuffer, ByteBuffer>());
    }
    return new MapWriter(partitions.get(partNum));
  }

  @Override
  public ByteBuffer getComparableKey(ByteBuffer key) {
    return key;
  }
}