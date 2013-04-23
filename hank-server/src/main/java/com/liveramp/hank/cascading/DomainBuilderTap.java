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
package com.liveramp.hank.cascading;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import com.liveramp.hank.hadoop.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * A sink-only tap to write tuples to Hank Domains.
 */
public class DomainBuilderTap extends Hfs {

  private static final long serialVersionUID = 1L;
  private final String domainName;
  private final Class<? extends DomainBuilderAbstractOutputFormat> outputFormatClass;

  public DomainBuilderTap(String keyFieldName, String valueFieldName, int versionNumber, DomainBuilderProperties properties) {
    // Set the output to the temporary output path
    super(new DomainBuilderScheme(DomainBuilderAssembly.PARTITION_FIELD_NAME,
        keyFieldName, valueFieldName), properties.getTmpOutputPath(versionNumber));
    this.domainName = properties.getDomainName();
    this.outputFormatClass = properties.getOutputFormatClass();
  }

  public void sinkConfInit(FlowProcess<JobConf> process, JobConf conf){
    super.sinkConfInit(process, conf);
    // Output Format
    conf.setOutputFormat(this.outputFormatClass);
    // Output Committer
    conf.setOutputCommitter(DomainBuilderOutputCommitter.class);
    // Set this tap's Domain name locally in the conf
    if (conf.get(DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_DOMAIN_NAME) != null) {
      throw new RuntimeException("Trying to set domain name configuration parameter to " + domainName +
          " but it was previously set to " + conf.get(DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_DOMAIN_NAME));
    } else {
      conf.set(DomainBuilderAbstractOutputFormat.CONF_PARAM_HANK_DOMAIN_NAME, domainName);
    }
  }

  @Override
  public void sourceConfInit(FlowProcess process, JobConf conf){
    throw new RuntimeException("DomainBuilderTap cannot be used as a source");
  }

  private static class DomainBuilderScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Void> {

    private static final long serialVersionUID = 1L;
    private final String partitionFieldName;
    private final String keyFieldName;
    private final String valueFieldName;

    public DomainBuilderScheme(String partitionFieldName, String keyFieldName, String valueFieldName) {
      super(new Fields(keyFieldName, valueFieldName), new Fields(partitionFieldName, keyFieldName, valueFieldName));
      this.partitionFieldName = partitionFieldName;
      this.keyFieldName = keyFieldName;
      this.valueFieldName = valueFieldName;
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> jobConfFlowProcess, Tap<JobConf, RecordReader, OutputCollector> jobConfRecordReaderOutputCollectorTap, JobConf entries) {
      throw new RuntimeException("DomainBuilderScheme cannot be used as a source.");
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> jobConfFlowProcess, Tap<JobConf, RecordReader, OutputCollector> jobConfRecordReaderOutputCollectorTap, JobConf entries) {
    }

    @Override
    public boolean source(FlowProcess<JobConf> jobConfFlowProcess, SourceCall<Object[], RecordReader> recordReaderSourceCall) throws IOException {
      throw new RuntimeException("DomainBuilderScheme cannot be used as a source.");
    }

    @Override
    public void sink(FlowProcess<JobConf> jobConfFlowProcess, SinkCall<Void, OutputCollector> sinkCall) throws IOException {
      TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
      IntWritable partition = new IntWritable(tupleEntry.getInteger(partitionFieldName));
      BytesWritable key = (BytesWritable) tupleEntry.getObject(keyFieldName);
      BytesWritable value = (BytesWritable) tupleEntry.getObject(valueFieldName);
      KeyAndPartitionWritable keyAndPartitionWritable = new KeyAndPartitionWritable(key, partition);
      ValueWritable valueWritable = new ValueWritable(value);
      sinkCall.getOutput().collect(keyAndPartitionWritable, valueWritable);
    }
  }
}
