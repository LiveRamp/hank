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
package com.rapleaf.hank.cascading;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import cascading.scheme.Scheme;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.rapleaf.hank.hadoop.DomainBuilderOutputFormat;
import com.rapleaf.hank.hadoop.DomainBuilderProperties;
import com.rapleaf.hank.hadoop.KeyAndPartitionWritable;
import com.rapleaf.hank.hadoop.ValueWritable;

/**
 * A sink-only tap to write tuples to Hank Domains.
 */
public class DomainBuilderTap extends Hfs {

  private static final long serialVersionUID = 1L;
  private final String outputPath;

  public DomainBuilderTap(String keyFieldName, String valueFieldName, DomainBuilderProperties properties) {
    super(new DomainBuilderScheme(DomainBuilderAssembly.PARTITION_FIELD_NAME, keyFieldName, valueFieldName, properties.getOutputFormatClass()), properties.getOutputPath());
    this.outputPath = properties.getOutputPath();
  }

  @Override
  public void sinkInit(JobConf conf) throws IOException {
    super.sinkInit(conf);
    conf.set(DomainBuilderOutputFormat.CONF_PARAM_HANK_OUTPUT_PATH, outputPath);
  }

  @Override
  public void sourceInit(JobConf conf) {
    throw new RuntimeException("DomainBuilderTap cannot be used as a source");
  }

  private static class DomainBuilderScheme extends Scheme {

    private static final long serialVersionUID = 1L;
    private final String partitionFieldName;
    private final String keyFieldName;
    private final String valueFieldName;
    private final Class<? extends DomainBuilderOutputFormat> outputFormatClass;

    public DomainBuilderScheme(String partitionFieldName, String keyFieldName, String valueFieldName, Class<? extends DomainBuilderOutputFormat> outputFormatClass) {
      super(new Fields(partitionFieldName, keyFieldName, valueFieldName), new Fields(keyFieldName, valueFieldName));
      this.partitionFieldName = partitionFieldName;
      this.keyFieldName = keyFieldName;
      this.valueFieldName = valueFieldName;
      this.outputFormatClass = outputFormatClass;
    }

    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
      IntWritable partition = new IntWritable(tupleEntry.getInteger(partitionFieldName));
      BytesWritable key = (BytesWritable) tupleEntry.get(keyFieldName);
      BytesWritable value = (BytesWritable) tupleEntry.get(valueFieldName);
      KeyAndPartitionWritable keyAndPartitionWritable = new KeyAndPartitionWritable(key, partition);
      ValueWritable valueWritable = new ValueWritable(value);
      outputCollector.collect(keyAndPartitionWritable, valueWritable);
    }

    @Override
    public void sinkInit(Tap tap, JobConf conf) throws IOException {
      conf.setOutputFormat(this.outputFormatClass);
    }

    @Override
    public Tuple source(Object key, Object value) {
      throw new RuntimeException("DomainBuilderScheme cannot be used as a source.");
    }

    @Override
    public void sourceInit(Tap tap, JobConf jobConf) throws IOException {
      throw new RuntimeException("DomainBuilderScheme cannot be used as a source.");
    }
  }
}