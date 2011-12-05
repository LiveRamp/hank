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

import com.rapleaf.hank.coordinator.Domain;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HadoopDomainCompactor extends AbstractHadoopDomainBuilder {

  private static final Logger LOG = Logger.getLogger(HadoopDomainCompactor.class);

  @Override
  protected void configureJob(JobConf conf) {
    // Input format
    conf.setInputFormat(HadoopDomainCompactorInputFormat.class);

    // Mappers
    conf.setMapperClass(HadoopDomainCompactorMapper.class);
    conf.setMapOutputKeyClass(NullWritable.class);
    conf.setMapOutputValueClass(NullWritable.class);

    // No reducers
    conf.setNumReduceTasks(0);

    // No output
    conf.setOutputFormat(NullOutputFormat.class);
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(NullWritable.class);
  }

  private static class HadoopDomainCompactorMapper implements Mapper<Text, IntWritable, NullWritable, NullWritable> {

    private Domain domain;
    private String tmpOutputPath;

    @Override
    public void configure(JobConf conf) {
      domain = DomainBuilderProperties.getDomain(conf);
      tmpOutputPath = DomainBuilderProperties.getTmpOutputPath(domain.getName(), conf);
    }

    @Override
    public void map(Text domainName, IntWritable partitionNumber,
                    OutputCollector<NullWritable, NullWritable> stringStringOutputCollector,
                    Reporter reporter) throws IOException {
      LOG.info("Compacting domain " + domainName.toString() + " partition " + partitionNumber.get()
          + " in " + tmpOutputPath);
      //TODO: Implement
      // domain.getStorageEngine().getCompactingUpdater()
    }

    @Override
    public void close() throws IOException {
    }
  }

  private static class HadoopDomainCompactorInputSplit implements InputSplit {

    private String domainName;
    private int partitionNumber;

    public HadoopDomainCompactorInputSplit() {
    }

    public HadoopDomainCompactorInputSplit(String domainName, int partitionNumber) {
      this.domainName = domainName;
      this.partitionNumber = partitionNumber;
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
      WritableUtils.writeString(dataOutput, domainName);
      WritableUtils.writeVInt(dataOutput, partitionNumber);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      domainName = WritableUtils.readString(dataInput);
      partitionNumber = WritableUtils.readVInt(dataInput);
    }

    public String getDomainName() {
      return domainName;
    }

    public int getPartitionNumber() {
      return partitionNumber;
    }
  }

  private static class HadoopDomainCompactorInputFormat implements InputFormat<Text, IntWritable> {

    private Domain domain;

    @Override
    public InputSplit[] getSplits(JobConf conf, int ignored) throws IOException {
      Domain domain = getCachedDomain(conf);
      InputSplit[] splits = new InputSplit[domain.getNumParts()];
      for (int partition = 0; partition < domain.getNumParts(); ++partition) {
        splits[partition] = new HadoopDomainCompactorInputSplit(domain.getName(), partition);
      }
      return splits;
    }

    @Override
    public RecordReader<Text, IntWritable> getRecordReader(InputSplit inputSplit,
                                                           JobConf conf,
                                                           Reporter reporter) throws IOException {
      HadoopDomainCompactorInputSplit split = (HadoopDomainCompactorInputSplit) inputSplit;
      return new HadoopDomainCompactorRecordReader(split);
    }

    private Domain getCachedDomain(JobConf conf) {
      if (domain == null) {
        domain = DomainBuilderProperties.getDomain(conf);
      }
      return domain;
    }
  }

  private static class HadoopDomainCompactorRecordReader implements RecordReader<Text, IntWritable> {

    private final HadoopDomainCompactorInputSplit split;
    private boolean done = false;

    public HadoopDomainCompactorRecordReader(HadoopDomainCompactorInputSplit split) {
      this.split = split;
    }

    @Override
    public boolean next(Text domainName, IntWritable partitionNumber) throws IOException {
      if (done) {
        return false;
      } else {
        domainName.set(split.getDomainName());
        partitionNumber.set(split.getPartitionNumber());
        done = true;
        return true;
      }
    }

    @Override
    public Text createKey() {
      return new Text();
    }

    @Override
    public IntWritable createValue() {
      return new IntWritable();
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
      if (done) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}
