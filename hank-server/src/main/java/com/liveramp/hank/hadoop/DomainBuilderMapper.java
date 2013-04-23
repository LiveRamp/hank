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

import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.RunWithCoordinator;
import com.liveramp.hank.coordinator.RunnableWithCoordinator;
import com.liveramp.hank.partitioner.Partitioner;
import com.liveramp.hank.storage.StorageEngine;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public abstract class DomainBuilderMapper<K, V> implements Mapper<K, V, KeyAndPartitionWritableComparable, ValueWritable> {

  private StorageEngine storageEngine;
  private Partitioner partitioner;
  private int numPartitions;

  public void configure(JobConf conf) {
    final String domainName = DomainBuilderProperties.getDomainName(conf);
    try {
      RunWithCoordinator.run(DomainBuilderProperties.getConfigurator(conf), new RunnableWithCoordinator() {
        @Override
        public void run(Coordinator coordinator) throws IOException {
          Domain domain = DomainBuilderProperties.getDomain(coordinator, domainName);
          storageEngine = domain.getStorageEngine();
          partitioner = domain.getPartitioner();
          numPartitions = domain.getNumParts();
        }
      });
    } catch (IOException e) {
      throw new RuntimeException("Failed to load configuration.", e);
    }
  }

  public final void map(K key, V value, OutputCollector<KeyAndPartitionWritableComparable, ValueWritable> outputCollector, Reporter reporter) throws IOException {
    KeyValuePair keyValue = buildHankKeyValue(key, value);
    KeyAndPartitionWritableComparable hankKeyWritableComparable =
        new KeyAndPartitionWritableComparable(storageEngine, partitioner, numPartitions, keyValue.getKey());
    ValueWritable hankValueWritable = new ValueWritable(keyValue.getValue());
    outputCollector.collect(hankKeyWritableComparable, hankValueWritable);
    reporter.progress();
  }

  public void close() throws IOException {
  }

  protected abstract KeyValuePair buildHankKeyValue(K key, V value);
}
