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
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.rapleaf.hank.hadoop.HankDomainOutputFormat;
import com.rapleaf.hank.hadoop.HankRecordWritable;

/**
 * A sink-only tap to write tuples to Hank Domains.
 */
public class HankDomainTap extends Hfs /*implements FlowListener*/ {

  private static final long serialVersionUID = 1L;
  private String keyFieldName;
  private String valueFieldName;
  private String outputPath;

  //private DomainConfig domainConfig;

  public HankDomainTap(String keyFieldName, String valueFieldName, String outputPath) {
    super(new HankDomainScheme(HankDomainAssembly.PARTITION_FIELD_NAME, keyFieldName, valueFieldName));
    this.keyFieldName = keyFieldName;
    this.valueFieldName = valueFieldName;
    this.outputPath = outputPath;
    //this.domainConfig = domainConfig;
  }

  @Override
  public void sinkInit(JobConf conf) throws IOException {
    super.sinkInit(conf);
    // Set output path in conf
    conf.set(HankDomainOutputFormat.CONF_PARAMETER_OUTPUT_PATH, outputPath);
    // throw new RuntimeException("Not yet implemented");
    //    domainConfig.getStorageEngine().getWriter(streamFactory, partNum, versionNumber, base);
  }

  @Override
  public void sourceInit(JobConf conf) {
    throw new RuntimeException("HankDomainTap cannot be used as a source");
  }

  private static class HankDomainScheme extends Scheme {

    private static final long serialVersionUID = 1L;
    private final String partitionFieldName;
    private final String keyFieldName;
    private final String valueFieldName;

    public HankDomainScheme(String partitionFieldName, String keyFieldName, String valueFieldName) {
      this.partitionFieldName = partitionFieldName;
      this.keyFieldName = keyFieldName;
      this.valueFieldName = valueFieldName;
    }

    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
      IntWritable partition = new IntWritable(tupleEntry.getInteger(partitionFieldName));
      BytesWritable key = (BytesWritable) tupleEntry.get(keyFieldName);
      BytesWritable value = (BytesWritable) tupleEntry.get(valueFieldName);

      HankRecordWritable record = new HankRecordWritable(key, value);
      outputCollector.collect(partition, record);
    }

    @Override
    public void sinkInit(Tap tap, JobConf jobConf) throws IOException {
      jobConf.setOutputFormat(HankDomainOutputFormat.class);
      // throw new RuntimeException("Not yet implemented");
    }

    @Override
    public Tuple source(Object key, Object value) {
      throw new RuntimeException("HankDomainScheme cannot be used as a source.");
    }

    @Override
    public void sourceInit(Tap tap, JobConf jobConf) throws IOException {
      throw new RuntimeException("HankDomainScheme cannot be used as a source.");
    }
  }

  //  @Override
  //  public boolean deletePath(JobConf arg0) throws IOException {
  //    throw new RuntimeException("Not yet implemented");
  //    // TODO Auto-generated method stub
  //  }
  //
  //  @Override
  //  public Path getPath() {
  //    throw new RuntimeException("Not yet implemented");
  //    // TODO Auto-generated method stub
  //  }
  //
  //  @Override
  //  public long getPathModified(JobConf arg0) throws IOException {
  //    throw new RuntimeException("Not yet implemented");
  //    // TODO Auto-generated method stub
  //  }
  //
  //  @Override
  //  public boolean makeDirs(JobConf arg0) throws IOException {
  //    throw new RuntimeException("Not yet implemented");
  //    // TODO Auto-generated method stub
  //  }
  //
  //  @Override
  //  public TupleEntryIterator openForRead(JobConf arg0) throws IOException {
  //    throw new RuntimeException("Not yet implemented");
  //    // TODO Auto-generated method stub
  //  }
  //
  //  @Override
  //  public TupleEntryCollector openForWrite(JobConf arg0) throws IOException {
  //    throw new RuntimeException("Not yet implemented");
  //    // TODO Auto-generated method stub
  //  }
  //
  //  @Override
  //  public boolean pathExists(JobConf arg0) throws IOException {
  //    throw new RuntimeException("Not yet implemented");
  //    // TODO Auto-generated method stub
  //  }

  /*
  @Override
  public void onCompleted(Flow flow) {
    throw new RuntimeException("Not yet implemented");
    // TODO Auto-generated method stub
  }

  @Override
  public void onStarting(Flow flow) {
    throw new RuntimeException("Not yet implemented");
    // TODO Auto-generated method stub

  }

  @Override
  public void onStopping(Flow flow) {
    throw new RuntimeException("Not yet implemented");
    // TODO Auto-generated method stub

  }

  @Override
  public boolean onThrowable(Flow flow, Throwable throwable) {
    throw new RuntimeException("Not yet implemented");
    // TODO Auto-generated method stub
  }
   */
}