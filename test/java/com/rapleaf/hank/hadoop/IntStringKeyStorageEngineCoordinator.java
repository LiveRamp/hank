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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import com.rapleaf.hank.config.Configurator;
import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.config.yaml.YamlClientConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.CoordinatorFactory;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.mock.MockCoordinator;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.storage.mock.MockStorageEngine;

// Integer String key storage engine.
// Store records (key, value) where key is an Integer's String representation
// The partition is key % numPartitions
// Format is "key value\n"
public class IntStringKeyStorageEngineCoordinator extends MockCoordinator {

  private final int numPartitions;

  public IntStringKeyStorageEngineCoordinator(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  private static class IntStringKeyWriter implements Writer {

    protected final OutputStream outputStream;

    IntStringKeyWriter(OutputStreamFactory streamFactory, int partNum,
                       int versionNumber, boolean base) throws IOException {
      this.outputStream = streamFactory.getOutputStream(partNum, Integer.toString(versionNumber) + "." + (base ? "base" : "nobase"));
    }

    public void write(ByteBuffer key, ByteBuffer value) throws IOException {
      outputStream.write(key.array(), key.position(), key.remaining());
      outputStream.write(" ".getBytes());
      outputStream.write(value.array(), value.position(), value.remaining());
      outputStream.write("\n".getBytes());
    }

    public void close() throws IOException {
      outputStream.close();
    }

    @Override
    public long getNumBytesWritten() {
      return 0;
    }

    @Override
    public long getNumRecordsWritten() {
      return 0;
    }
  }

  private static class IntStringKeyModPartitioner implements Partitioner {

    public int partition(ByteBuffer key, int numPartitions) {
      String keyString = new String(key.array(), key.position(), key.remaining());
      Integer keyInteger = Integer.valueOf(keyString);
      return keyInteger % numPartitions;
    }
  }

  private static class IntStringKeyStorageEngine extends MockStorageEngine {
    @Override
    public Writer getWriter(OutputStreamFactory streamFactory, int partNum,
                            int versionNumber, boolean base) throws IOException {
      return new IntStringKeyWriter(streamFactory, partNum, versionNumber, base);
    }

    @Override
    public ByteBuffer getComparableKey(ByteBuffer key) {
      return key;
    }
  }

  public static class Factory implements CoordinatorFactory {
    public Coordinator getCoordinator(Map<String, Object> options) {
      Integer numPartitions = (Integer) options.get("num_partitions");
      if (numPartitions != null) {
        return new IntStringKeyStorageEngineCoordinator(numPartitions);
      } else {
        return new IntStringKeyStorageEngineCoordinator(1);
      }
    }
  }

  @Override
  public Domain getDomain(String domainName) {
    return new MockDomain(domainName, 1, numPartitions, new IntStringKeyModPartitioner(), new IntStringKeyStorageEngine(), null, new MockDomainVersion(0, null));
  }

  static public Configurator getConfigurator(int numPartitions) {
    YamlClientConfigurator configurator = new YamlClientConfigurator();
    try {
      configurator.loadFromYaml("coordinator:\n  factory: com.rapleaf.hank.hadoop.IntStringKeyStorageEngineCoordinator$Factory\n  options:\n    num_partitions: " + numPartitions + "\n");
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException(e);
    }
    return configurator;
  }
}
