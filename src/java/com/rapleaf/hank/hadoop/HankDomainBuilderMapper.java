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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.rapleaf.hank.config.Configurator;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.partitioner.Partitioner;

public abstract class HankDomainBuilderMapper<K, V> implements Mapper<K, V, IntWritable, HankRecordWritable> {

  private Partitioner partitioner;

  @Override
  public void configure(JobConf conf) {
    Configurator configurator = new HadoopJobConfConfigurator(conf);
    String domainName = HankDomainOutputFormat.getRequiredConfigurationItem(HankDomainOutputFormat.CONF_PARAM_HANK_DOMAIN_NAME, "Hank domain name", conf);
    try {
      partitioner = configurator.getCoordinator().getDomainConfig(domainName).getPartitioner();
    } catch (DataNotFoundException e) {
      throw new RuntimeException("Failed to load domain config for domain: " + domainName, e);
    }
  }

  @Override
  public final void map(K key, V value, OutputCollector<IntWritable, HankRecordWritable> outputCollector, Reporter reporter) throws IOException {
    HankRecordWritable hankRecord = this.buildHankRecord(key, value);
    Integer partition = partitioner.partition(hankRecord.getKey());
    outputCollector.collect(new IntWritable(partition), hankRecord);
  }

  @Override
  public void close() throws IOException {

  }

  protected abstract HankRecordWritable buildHankRecord(K key, V value);
}
