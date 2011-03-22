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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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

public class TestHankDomainOutputFormat extends TestCase {
  private FileSystem fs;

  static final String TEST_DIR = "/tmp/test_" + TestHankDomainOutputFormat.class.getName();
  static final String OUTPUT_DIR = TEST_DIR + "/output";

  public TestHankDomainOutputFormat() throws IOException {
    super();
    this.fs = FileSystem.get(new Configuration());
  }

  @Override
  public void setUp() {
    try {
      fs.delete(new Path(TEST_DIR), true);
      fs.mkdirs(new Path(TEST_DIR));
      outputFile(fs, TEST_DIR + "/a", "1 k1 v1\n1 k2 v2\n2 k3 v3");
    } catch (IOException e) {
      throw new RuntimeException("Could not set up tests.");
    }
  }

  public void testFailIfOutputExists() throws IOException {
    fs.create(new Path(OUTPUT_DIR));
    try {
      JobClient.runJob(getConf(TEST_DIR + "/a", OUTPUT_DIR));
      fail("Should fail when output exists");
    } catch (FileAlreadyExistsException e) {
    }
  }

  public void testOutput() throws IOException {
    JobClient.runJob(getConf(TEST_DIR + "/a", OUTPUT_DIR));
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

  private JobConf getConf(String inputPath, String outputPath) {
    JobConf conf = new JobConf();
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(HankRecordWritable.class);
    conf.setMapperClass(TestMapper.class);
    conf.setReducerClass(IdentityReducer.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(HankDomainOutputFormat.class);
    conf.set(HankDomainOutputFormat.CONF_PARAMETER_OUTPUT_PATH, outputPath);
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

  protected void outputFile(FileSystem fs, String path, String output)
  throws IOException {
    FSDataOutputStream os = fs.create(new Path(path));
    os.write(output.getBytes());
    os.close();
  }

  protected String getContents(FileSystem fs, String path) throws IOException {
    FSDataInputStream in = fs.open(new Path(path));
    StringBuilder builder = new StringBuilder();
    byte[] buffer = new byte[1024];
    int bytesRead;
    while ((bytesRead = in.read(buffer)) > 0) {
      builder.append(new String(buffer, 0, bytesRead));
    }
    in.close();
    return builder.toString();
  }
}
