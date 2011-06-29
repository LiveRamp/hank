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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.rapleaf.hank.coordinator.Domain;

public abstract class DomainBuilderMapper<K, V> implements Mapper<K, V, KeyAndPartitionWritableComparable, ValueWritable> {

  private Domain domain;

  public void configure(JobConf conf) {
    domain = DomainBuilderProperties.getDomain(conf);
  }

  public final void map(K key, V value, OutputCollector<KeyAndPartitionWritableComparable, ValueWritable> outputCollector, Reporter reporter) throws IOException {
    KeyValuePair keyValue = buildHankKeyValue(key, value);
    KeyAndPartitionWritableComparable hankKeyWritableComparable = new KeyAndPartitionWritableComparable(domain, keyValue.getKey());
    ValueWritable hankValueWritable = new ValueWritable(keyValue.getValue());
    outputCollector.collect(hankKeyWritableComparable, hankValueWritable);
  }

  public void close() throws IOException {
  }

  protected abstract KeyValuePair buildHankKeyValue(K key, V value);
}
