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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.storage.StorageEngine;

// This class should be used exclusively for testing. It holds all written records in
// memory in a Map. It is not thread safe.
public class DomainBuilderInMemoryOutputFormat extends DomainBuilderOutputFormat {

  private static Map<Integer, Map<ByteBuffer, ByteBuffer>> partitions = new HashMap<Integer, Map<ByteBuffer, ByteBuffer>>();

  public Map<Integer, Map<ByteBuffer, ByteBuffer>> getPartitions() {
    return partitions;
  }

  @Override
  public void checkOutputSpecs(FileSystem fs, JobConf conf) throws IOException {
  }

  @Override
  public RecordWriter<KeyAndPartitionWritable, ValueWritable> getRecordWriter(
      FileSystem fs, JobConf conf, String name, Progressable progressable) throws IOException {
    // Load config
    DomainConfig domainConfig = JobConfConfigurator.getDomainConfig(conf);
    // Build RecordWriter with the DomainConfig
    return new DomainBuilderRecordWriter(domainConfig);
  }

  private static class DomainBuilderRecordWriter implements RecordWriter<KeyAndPartitionWritable, ValueWritable> {

    private static final String TMP_DIRECTORY_NAME = "_tmp_HankDomainRecordWriter";

    private final DomainConfig domainConfig;
    private final StorageEngine storageEngine;

    DomainBuilderRecordWriter(DomainConfig domainConfig) {
      this.domainConfig = domainConfig;
      this.storageEngine = domainConfig.getStorageEngine();
    }

    @Override
    public void close(Reporter reporter) throws IOException {
    }

    @Override
    public void write(KeyAndPartitionWritable key, ValueWritable value) {
      if (!partitions.containsKey(key.getPartition())) {
        partitions.put(key.getPartition(), new HashMap<ByteBuffer, ByteBuffer>());
      }
      if (partitions.get(key.getPartition()).containsKey(key.getKey())) {
        throw new RuntimeException("Duplicated key in partition " + key.getPartition() + ": " + key.getKey().toString());
      }
      partitions.get(key.getPartition()).put(key.getKey(), value.getAsByteBuffer());
    }
  }
}
