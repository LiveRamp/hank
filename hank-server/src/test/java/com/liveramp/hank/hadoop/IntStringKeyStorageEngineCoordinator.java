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

package com.liveramp.hank.hadoop;

import com.liveramp.hank.config.CoordinatorConfigurator;
import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.yaml.YamlClientConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.CoordinatorFactory;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.mock.MockCoordinator;
import com.liveramp.hank.coordinator.mock.MockDomain;
import com.liveramp.hank.coordinator.mock.MockDomainVersion;
import com.liveramp.hank.partitioner.Partitioner;
import com.liveramp.hank.storage.PartitionRemoteFileOps;
import com.liveramp.hank.storage.Writer;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.liveramp.hank.storage.mock.MockStorageEngine;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

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

    IntStringKeyWriter(DomainVersion domainVersion,
                       PartitionRemoteFileOps partitionRemoteFileOps,
                       int partNum) throws IOException {
      IncrementalDomainVersionProperties domainVersionProperties =
          (IncrementalDomainVersionProperties) domainVersion.getProperties();
      if (domainVersionProperties == null) {
        throw new RuntimeException("IntStringKeyWriter needs a non null DomainVersionProperties");
      }
      this.outputStream = partitionRemoteFileOps.getOutputStream(Integer.toString(domainVersion.getVersionNumber()) + "." + (domainVersionProperties.isBase() ? "base" : "nobase"));
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
    public Writer getWriter(DomainVersion domainVersion,
                            PartitionRemoteFileOps partitionRemoteFileOps,
                            int partitionNumber) throws IOException {
      return new IntStringKeyWriter(domainVersion, partitionRemoteFileOps, partitionNumber);
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
    return new MockDomain(domainName,
        1,
        numPartitions,
        new IntStringKeyModPartitioner(),
        new IntStringKeyStorageEngine(),
        null,
        new MockDomainVersion(0, null, new IncrementalDomainVersionProperties.Base()));
  }

  static public CoordinatorConfigurator getConfigurator(int numPartitions) {
    YamlClientConfigurator configurator = new YamlClientConfigurator();
    try {
      configurator.loadFromYaml("coordinator:\n  factory: com.liveramp.hank.hadoop.IntStringKeyStorageEngineCoordinator$Factory\n  options:\n    num_partitions: " + numPartitions + "\n");
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException(e);
    }
    return configurator;
  }
}
