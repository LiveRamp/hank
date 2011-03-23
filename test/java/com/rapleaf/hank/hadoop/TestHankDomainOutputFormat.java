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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import com.rapleaf.hank.HadoopTestCase;
import com.rapleaf.hank.storage.cueball.Cueball;

public class TestHankDomainOutputFormat extends HadoopTestCase {

  public TestHankDomainOutputFormat() throws IOException {
    super();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    outputFile(fs, TEST_DIR + "/a", "1 k1 v1\n1 k2 v2\n2 k3 v3");
  }

  public void testFailIfOutputExists() throws IOException {
    fs.create(new Path(OUTPUT_DIR));
    try {
      JobClient.runJob(getConf(TEST_DIR + "/a", OUTPUT_DIR, Cueball.class.getName()));
      fail("Should fail when output exists");
    } catch (FileAlreadyExistsException e) {
    }
  }

  public void testOutput() throws IOException {
    JobClient.runJob(getConf(TEST_DIR + "/a", OUTPUT_DIR, Cueball.class.getName()));
    String p1 = getContents(fs, HadoopFSOutputStreamFactory.getPath(OUTPUT_DIR, 1, "00001.base.cueball"));
    String p2 = getContents(fs, HadoopFSOutputStreamFactory.getPath(OUTPUT_DIR, 2, "00001.base.cueball"));
    // hash(k1),v1
    assertEquals(-67, p1.getBytes()[0]);
    assertEquals(-8, p1.getBytes()[1]);
    assertEquals("v1", p1.substring(2, 4));

    // hash(k2),v2
    assertEquals(119, p1.getBytes()[4]);
    assertEquals(105, p1.getBytes()[5]);
    assertEquals("v2", p1.substring(6, 8));

    // hash(k3),v3
    assertEquals(-121, p2.getBytes()[0]);
    assertEquals(-13, p2.getBytes()[1]);
    assertEquals("v3", p2.substring(2, 4));
  }

  private JobConf getConf(String inputPath, String outputPath, String storageEngine) {
    JobConf conf = new JobConf();
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(HankRecordWritable.class);
    conf.setMapperClass(TestMapper.class);
    conf.setReducerClass(IdentityReducer.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(HankDomainOutputFormat.class);
    conf.set(HankDomainOutputFormat.CONF_PARAMETER_OUTPUT_PATH, outputPath);
    conf.set(HankDomainOutputFormat.CONF_PARAMETER_STORAGE_ENGINE, storageEngine);
    FileInputFormat.setInputPaths(conf, inputPath);
    return conf;
  }

  // Converts text file lines "<partition> <key> <value>" to the corresponding
  // HankRecordWritable object
  private static class TestMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, HankRecordWritable> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, HankRecordWritable> outputCollector, Reporter reporter) throws IOException {
      String[] splits = value.toString().split(" ");
      if (splits.length != 3) {
        throw new RuntimeException("Input text file must be lines like \"<partition> <key> <value>\"");
      }
      HankRecordWritable retValue = new HankRecordWritable(splits[1].getBytes(), splits[2].getBytes());
      outputCollector.collect(new IntWritable(Integer.valueOf(splits[0])), retValue);
    }
  }
}
