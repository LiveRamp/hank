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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import com.rapleaf.hank.HadoopTestCase;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.CoordinatorFactory;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.MockCoordinator;
import com.rapleaf.hank.coordinator.MockDomainConfig;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.partitioner.Murmur64Partitioner;
import com.rapleaf.hank.storage.MockStorageEngine;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.Writer;

public class TestHankDomainOutputFormat extends HadoopTestCase {

  private final String DOMAIN_A_NAME = "a";

  public TestHankDomainOutputFormat() throws IOException {
    super();
  }

  private static class LocalMockWriter implements Writer {

    protected final OutputStream outputStream;

    LocalMockWriter(OutputStreamFactory streamFactory, int partNum,
        int versionNumber, boolean base) throws IOException {
      this.outputStream = streamFactory.getOutputStream(partNum, Integer.toString(versionNumber) + "." + (base ? "base" : "nobase"));
    }

    @Override
    public void write(ByteBuffer key, ByteBuffer value) throws IOException {
      this.outputStream.write(key.array(), key.position(), key.remaining());
      outputStream.write(" ".getBytes());
      outputStream.write(value.array(), value.position(), value.remaining());
      outputStream.write("\n".getBytes());
    }

    @Override
    public void close() throws IOException {
      outputStream.close();
    }
  }

  private static class LocalMockStorageEngine extends MockStorageEngine {
    @Override
    public Writer getWriter(OutputStreamFactory streamFactory, int partNum,
        int versionNumber, boolean base) throws IOException {
      return new LocalMockWriter(streamFactory, partNum, versionNumber, base);
    }
  }

  private static class LocalMockCoordinator extends MockCoordinator {

    public static class Factory implements CoordinatorFactory {
      @Override
      public Coordinator getCoordinator(Map<String, Object> options) {
        return new LocalMockCoordinator();
      }
    }

    @Override
    public DomainConfig getDomainConfig(String domainName) throws DataNotFoundException {
      return new MockDomainConfig(domainName, 2, new Murmur64Partitioner(), new LocalMockStorageEngine(), 0);
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    outputFile(fs, INPUT_DIR + "/" + DOMAIN_A_NAME, "1 k1 v1\n1 k2 v2\n2 k3 v3");
  }

  private String getConfiguration() {
    return "coordinator:\n  factory: com.rapleaf.hank.hadoop.TestHankDomainOutputFormat$LocalMockCoordinator$Factory\n" +
    "  options:\n";
  }

  public void testFailIfOutputExists() throws IOException {
    fs.create(new Path(OUTPUT_DIR));
    try {
      JobClient.runJob(BuildHankDomain.createJobConfiguration(DOMAIN_A_NAME, INPUT_DIR + "/" + DOMAIN_A_NAME, TextInputFormat.class, TestMapper.class, getConfiguration(), OUTPUT_DIR));
      fail("Should fail when output exists");
    } catch (FileAlreadyExistsException e) {
    }
  }

  public void testOutput() throws IOException {
    JobClient.runJob(BuildHankDomain.createJobConfiguration(DOMAIN_A_NAME, INPUT_DIR + "/" + DOMAIN_A_NAME, TextInputFormat.class, TestMapper.class, getConfiguration(), OUTPUT_DIR));
    String p1 = getContents(fs, HadoopFSOutputStreamFactory.getPath(OUTPUT_DIR, 1, "0.base"));
    String p2 = getContents(fs, HadoopFSOutputStreamFactory.getPath(OUTPUT_DIR, 2, "0.base"));

    assertEquals("k1 v1\nk2 v2\n", p1);
    assertEquals("k3 v3\n", p2);
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
