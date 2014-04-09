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
import cascading.tap.hadoop.io.RecordReaderIterator;
import cascading.tuple.*;
import com.liveramp.hank.hadoop.DomainBuilderProperties;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PartitionMarkerTap extends Tap<JobConf, RecordReader, OutputCollector> {

  private final String domainName;

  public PartitionMarkerTap(String domainName, String keyFieldName, String valueFieldName) {
    super(new PartitionMarkerScheme(keyFieldName, valueFieldName));
    this.domainName = domainName;
  }

  @Override
  public String getIdentifier() {
    return "__hank_partition_markers_for_" + domainName;
  }

  @Override
  public TupleEntryIterator openForRead(FlowProcess<JobConf> jobConfFlowProcess, RecordReader recordReader) throws IOException {
    return new TupleEntrySchemeIterator(jobConfFlowProcess, this.getScheme(), new RecordReaderIterator(recordReader));
  }

  @Override
  public TupleEntryCollector openForWrite(FlowProcess<JobConf> jobConfFlowProcess, OutputCollector outputCollector) throws IOException {
    throw new RuntimeException("PartitionMarkerTap cannot be used as a sink.");  }

  @Override
  public boolean createResource(JobConf entries) throws IOException {
    return true;
  }

  @Override
  public boolean deleteResource(JobConf entries) throws IOException {
    return true;
  }

  @Override
  public boolean resourceExists(JobConf entries) throws IOException {
    return false;
  }

  @Override
  public long getModifiedTime(JobConf entries) throws IOException {
    return 0;
  }

  private static class PartitionMarkerRecordReader implements RecordReader<IntWritable, IntWritable> {

    private int currentPartition = 0;
    private final int numPartitions;

    public PartitionMarkerRecordReader(int numPartitions) {
      this.numPartitions = numPartitions;
    }

    @Override
    public boolean next(IntWritable key, IntWritable value) throws IOException {
      // value is not used
      if (currentPartition < numPartitions) {
        key.set(currentPartition);
        currentPartition += 1;
        return currentPartition <= numPartitions;
      } else {
        return false;
      }
    }

    @Override
    public IntWritable createKey() {
      return new IntWritable();
    }

    @Override
    public IntWritable createValue() {
      return new IntWritable();
    }

    @Override
    public long getPos() throws IOException {
      return currentPartition;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
      return (float) currentPartition / (float) numPartitions;
    }
  }

  private static class PartitionMarkerInputFormat implements InputFormat<IntWritable, IntWritable> {

    @Override
    public InputSplit[] getSplits(JobConf conf, int ignored) throws IOException {
      String domainName = DomainBuilderProperties.getDomainName(conf);
      int numPartitions = DomainBuilderProperties.getNumPartitions(domainName, conf);
      return new InputSplit[]{new PartitionMarkerInputSplit(numPartitions)};
    }

    @Override
    public RecordReader<IntWritable, IntWritable> getRecordReader(InputSplit inputSplit,
                                                                  JobConf conf,
                                                                  Reporter reporter) throws IOException {
      return new PartitionMarkerRecordReader(((PartitionMarkerInputSplit) inputSplit).getNumPartitions());
    }
  }

  private static class PartitionMarkerScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Void> {

    public PartitionMarkerScheme(String keyFieldName, String valueFieldName) {
      super(new Fields(keyFieldName,
          valueFieldName,
          DomainBuilderAssembly.PARTITION_FIELD_NAME,
          DomainBuilderAssembly.COMPARABLE_KEY_FIELD_NAME));
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> jobConfFlowProcess, Tap<JobConf, RecordReader, OutputCollector> jobConfOutputCollectorTap, JobConf entries) {
      entries.setInputFormat(PartitionMarkerInputFormat.class);
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> jobConfFlowProcess, Tap<JobConf, RecordReader, OutputCollector> jobConfOutputCollectorTap, JobConf entries) {
      throw new RuntimeException("PartitionMarkerScheme cannot be used as a sink.");
    }

    @Override
    public boolean source(FlowProcess<JobConf> jobConfFlowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {

      Tuple tuple = sourceCall.getIncomingEntry().getTuple();
      IntWritable partition = (IntWritable) sourceCall.getInput().createKey();

      boolean result = sourceCall.getInput().next(partition, null);
      if(!result){
        return false;
      }

      tuple.set(0, null);
      tuple.set(1, null);
      tuple.set(2, partition);
      tuple.set(3, null);

      return true;
    }

    @Override
    public void sink(FlowProcess<JobConf> jobConfFlowProcess, SinkCall<Void, OutputCollector> outputCollectorSinkCall) throws IOException {
      throw new RuntimeException("PartitionMarkerScheme cannot be used as a sink.");
    }
  }

  private static class PartitionMarkerInputSplit implements InputSplit {

    private Integer numPartitions;

    public PartitionMarkerInputSplit() {
      this.numPartitions = null;
    }

    public PartitionMarkerInputSplit(int numPartitions) {
      this.numPartitions = numPartitions;
    }

    @Override
    public long getLength() throws IOException {
      return 1;
    }

    @Override
    public String[] getLocations() throws IOException {
      return new String[]{};
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      WritableUtils.writeVInt(dataOutput, numPartitions);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      numPartitions = WritableUtils.readVInt(dataInput);
    }

    public int getNumPartitions() {
      return numPartitions;
    }
  }
}
