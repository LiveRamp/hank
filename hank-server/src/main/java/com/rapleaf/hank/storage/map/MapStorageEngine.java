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

package com.rapleaf.hank.storage.map;

import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.storage.NoOpPartitionRemoteFileOps;
import com.rapleaf.hank.storage.PartitionRemoteFileOps;
import com.rapleaf.hank.storage.PartitionRemoteFileOpsFactory;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.storage.mock.MockStorageEngine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

// Storage engine used for testing. Stores key-value pairs in a static
// partition map instead of writing them to an output stream. It is not thread
// safe.
public class MapStorageEngine extends MockStorageEngine {

  private static final Map<String, Map<Integer, Map<ByteBuffer, ByteBuffer>>> partitions =
      new HashMap<String, Map<Integer, Map<ByteBuffer, ByteBuffer>>>();
  private static final Map<String, Map<String, Object>> options =
      new HashMap<String, Map<String, Object>>();

  private final String domainName;

  public MapStorageEngine(String domainName) {
    this.domainName = domainName;
  }

  // Use clear to clear all data (e.g. before a test)
  public static void clear() {
    partitions.clear();
    options.clear();
  }

  public static Map<Integer, Map<ByteBuffer, ByteBuffer>> getPartitions(String domainName) {
    return partitions.get(domainName);
  }

  public static Map<String, Object> getOptions(String domainName) {
    if (!options.containsKey(domainName)) {
      options.put(domainName, new HashMap<String, Object>());
    }
    return options.get(domainName);
  }

  @Override
  public Writer getWriter(DomainVersion domainVersion,
                          PartitionRemoteFileOps partitionRemoteFileOps,
                          int partitionNumber) throws IOException {
    if (!partitions.containsKey(domainName)) {
      partitions.put(domainName, new HashMap<Integer, Map<ByteBuffer, ByteBuffer>>());
    }
    if (!partitions.get(domainName).containsKey(partitionNumber)) {
      partitions.get(domainName).put(partitionNumber, new HashMap<ByteBuffer, ByteBuffer>());
    }
    return new MapWriter(partitions.get(domainName).get(partitionNumber));
  }

  @Override
  public ByteBuffer getComparableKey(ByteBuffer key) {
    return key;
  }

  @Override
  public PartitionRemoteFileOpsFactory getPartitionRemoteFileOpsFactory() {
    return new NoOpPartitionRemoteFileOps.Factory();
  }
}
