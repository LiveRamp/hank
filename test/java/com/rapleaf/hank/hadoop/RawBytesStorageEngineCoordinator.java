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

package com.rapleaf.hank.hadoop;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.CoordinatorFactory;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.MockCoordinator;
import com.rapleaf.hank.coordinator.MockDomainConfig;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.MockStorageEngine;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.Writer;


// Raw bytes storage engine
// Simplistic storage engine that stores (key, value) pairs in a raw binary form
// BytesWritable interface is used to write keys and values sequentially
public class RawBytesStorageEngineCoordinator extends MockCoordinator {

  private static class RawBytesWriter implements Writer {

    protected final DataOutputStream dataOutputStream;

    RawBytesWriter(OutputStreamFactory streamFactory, int partNum,
        int versionNumber, boolean base) throws IOException {
      this.dataOutputStream = new DataOutputStream(streamFactory.getOutputStream(partNum, Integer.toString(versionNumber) + "." + (base ? "base" : "nobase")));
    }

    @Override
    public void write(ByteBuffer key, ByteBuffer value) throws IOException {
      // Create key bytes
      byte[] keyBytes = new byte[key.remaining()];
      System.arraycopy(key.array(), key.arrayOffset(), keyBytes, 0, key.remaining());
      // Create value bytes
      byte[] valueBytes = new byte[value.remaining()];
      System.arraycopy(value.array(), value.arrayOffset(), valueBytes, 0, value.remaining());
      // Write key and value bytes
      new BytesWritable(keyBytes).write(dataOutputStream);
      new BytesWritable(valueBytes).write(dataOutputStream);
    }

    @Override
    public void close() throws IOException {
      dataOutputStream.close();
    }
  }

  private static class RawBytesPartitioner implements Partitioner {

    @Override
    public int partition(ByteBuffer key) {
      return 0;
    }
  }

  private static class RawBytesStorageEngine extends MockStorageEngine {
    @Override
    public Writer getWriter(OutputStreamFactory streamFactory, int partNum,
        int versionNumber, boolean base) throws IOException {
      return new RawBytesWriter(streamFactory, partNum, versionNumber, base);
    }

    @Override
    public ByteBuffer getComparableKey(ByteBuffer key) {
      return key;
    }
  }

  public static class Factory implements CoordinatorFactory {
    @Override
    public Coordinator getCoordinator(Map<String, Object> options) {
      return new RawBytesStorageEngineCoordinator();
    }
  }

  @Override
  public DomainConfig getDomainConfig(String domainName) throws DataNotFoundException {
    return new MockDomainConfig(domainName, 1, new RawBytesPartitioner(), new RawBytesStorageEngine(), 0);
  }

  static public String getConfiguration() {
    return "coordinator:\n  factory: com.rapleaf.hank.hadoop.RawBytesStorageEngineCoordinator$Factory\n  options:\n";
  }
}
