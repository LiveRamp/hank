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

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.*;
import com.rapleaf.hank.hadoop.DomainBuilderProperties;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PartitionMarkerTap extends Tap {

  private final String domainName;

  public PartitionMarkerTap(String domainName, String keyFieldName, String valueFieldName) {
    super(new PartitionMarkerScheme(keyFieldName, valueFieldName));
    this.domainName = domainName;
  }

  @Override
  public Path getPath() {
    return new Path("__hank_partition_markers_for_" + domainName);
  }

  @Override
  public TupleEntryIterator openForRead(JobConf conf) throws IOException {
    return null;
  }

  @Override
  public TupleEntryCollector openForWrite(JobConf conf) throws IOException {
    throw new RuntimeException("PartitionMarkerTap cannot be used as a sink.");
  }

  @Override
  public boolean makeDirs(JobConf conf) throws IOException {
    return true;
  }

  @Override
  public boolean deletePath(JobConf conf) throws IOException {
    return true;
  }

  @Override
  public boolean pathExists(JobConf conf) throws IOException {
    return false;
  }

  @Override
  public long getPathModified(JobConf conf) throws IOException {
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

  private static class PartitionMarkerScheme extends Scheme {

    public PartitionMarkerScheme(String keyFieldName, String valueFieldName) {
      super(new Fields(keyFieldName,
          valueFieldName,
          DomainBuilderAssembly.PARTITION_FIELD_NAME,
          DomainBuilderAssembly.COMPARABLE_KEY_FIELD_NAME));
    }

    @Override
    public void sourceInit(Tap tap, JobConf conf) throws IOException {
      conf.setInputFormat(PartitionMarkerInputFormat.class);
    }

    @Override
    public void sinkInit(Tap tap, JobConf conf) throws IOException {
      throw new RuntimeException("PartitionMarkerScheme cannot be used as a sink.");
    }

    @Override
    public Tuple source(Object key, Object value) {
      IntWritable partition = (IntWritable) key;
      return new Tuple(null, null, partition, null);
    }

    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
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
