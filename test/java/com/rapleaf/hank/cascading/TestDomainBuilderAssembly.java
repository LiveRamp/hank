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
import java.util.Properties;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;

import cascading.flow.FlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.rapleaf.hank.HadoopTestCase;
import com.rapleaf.hank.hadoop.DomainBuilderOutputFormat;
import com.rapleaf.hank.hadoop.HDFSOutputStreamFactory;
import com.rapleaf.hank.hadoop.TestHadoopDomainBuilder;

public class TestDomainBuilderAssembly extends HadoopTestCase {

  final String INPUT_PATH_A;
  final String OUTPUT_PATH_A;
  final String DOMAIN_A_NAME;

  public TestDomainBuilderAssembly() throws IOException {
    super(TestDomainBuilderAssembly.class);
    DOMAIN_A_NAME = "a";
    INPUT_PATH_A = INPUT_DIR + "/" + DOMAIN_A_NAME;
    OUTPUT_PATH_A = OUTPUT_DIR + "/" + DOMAIN_A_NAME;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    try {
      createInputs();
    } catch (IOException e) {
      throw new RuntimeException("Could not set up testcase.", e);
    }
  }

  private void createInputs() throws IOException {
    // A
    Tap inputTap = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH_A);
    TupleEntryCollector coll = inputTap.openForWrite(new JobConf());
    coll.add(getTT("2", "v2"));
    coll.add(getTT("0", "v0"));
    coll.add(getTT("3", "v3"));
    coll.add(getTT("1", "v1"));
    coll.add(getTT("4", "v4"));
    coll.close();
  }

  private Pipe getPipe(String configuration, String domainName, DomainBuilderTap outputTap) {
    Pipe pipe = new Pipe("pipe");
    pipe = new Each(pipe, new Fields("key", "value"), new Identity());
    pipe = new DomainBuilderAssembly(configuration, domainName, pipe, "key", "value");
    return pipe;
  }

  private Tuple getTT(String b1, String b2) {
    return new Tuple(new BytesWritable(b1.getBytes()),
        new BytesWritable(b2.getBytes()));
  }

  public void testMain() throws IOException {
    Tap inputTap = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH_A);
    DomainBuilderTap outputTap = new DomainBuilderTap("key", "value", OUTPUT_PATH_A);
    String configuration = TestHadoopDomainBuilder.getConfiguration();
    Pipe pipe = getPipe(configuration, "", outputTap);

    Properties properties = new Properties();
    DomainBuilderOutputFormat.setProperties(properties, configuration, DOMAIN_A_NAME, OUTPUT_PATH_A);
    new FlowConnector(properties).connect(inputTap, outputTap, pipe).complete();

    // Check output
    String p1 = getContents(fs, HDFSOutputStreamFactory.getPath(OUTPUT_DIR + "/" + DOMAIN_A_NAME, 0, "0.base"));
    String p2 = getContents(fs, HDFSOutputStreamFactory.getPath(OUTPUT_DIR + "/" + DOMAIN_A_NAME, 1, "0.base"));
    assertEquals("0 v0\n2 v2\n4 v4\n", p1);
    assertEquals("1 v1\n3 v3\n", p2);
  }
}