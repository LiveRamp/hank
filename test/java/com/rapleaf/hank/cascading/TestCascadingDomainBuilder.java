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

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import com.rapleaf.hank.hadoop.DomainBuilderProperties;
import com.rapleaf.hank.hadoop.HDFSOutputStreamFactory;
import com.rapleaf.hank.hadoop.HadoopTestCase;
import com.rapleaf.hank.hadoop.IntStringKeyStorageEngineCoordinator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TestCascadingDomainBuilder extends HadoopTestCase {

  private final String DOMAIN_A_NAME = "a";
  private final String INPUT_PATH_A = INPUT_DIR + "/" + DOMAIN_A_NAME;
  private final String OUTPUT_PATH_A = OUTPUT_DIR + "/" + DOMAIN_A_NAME;

  private final String DOMAIN_B_NAME = "b";
  private final String INPUT_PATH_B = INPUT_DIR + "/" + DOMAIN_B_NAME;
  private final String OUTPUT_PATH_B = OUTPUT_DIR + "/" + DOMAIN_B_NAME;

  public TestCascadingDomainBuilder() throws IOException {
    super(TestCascadingDomainBuilder.class);
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

  private void writeSequenceFile(String path, Fields fields, Tuple... tuples) throws IOException {
    Tap tap = new Hfs(new SequenceFile(fields), path);
    TupleEntryCollector coll = tap.openForWrite(new JobConf());
    for (Tuple t : tuples) {
      coll.add(t);
    }
    coll.close();
  }

  private void createInputs() throws IOException {
    // A
    writeSequenceFile(INPUT_PATH_A, new Fields("key", "value"),
        getTT("2", "v2"),
        getTT("0", "v0"),
        getTT("3", "v3"),
        getTT("1", "v1"),
        getTT("4", "v4"));
    // B
    writeSequenceFile(INPUT_PATH_B, new Fields("key", "value"),
        getTT("12", "v2"),
        getTT("10", "v0"),
        getTT("13", "v3"),
        getTT("11", "v1"),
        getTT("14", "v4"));
  }

  private Pipe getPipe(String name) {
    Pipe pipe = new Pipe(name);
    pipe = new Each(pipe, new Fields("key", "value"), new Identity());
    return pipe;
  }

  private Tuple getTT(String b1, String b2) {
    return new Tuple(new BytesWritable(b1.getBytes()),
        new BytesWritable(b2.getBytes()));
  }

  public void testMain() throws IOException {
    DomainBuilderProperties properties = new DomainBuilderProperties(DOMAIN_A_NAME,
        false, IntStringKeyStorageEngineCoordinator.getConfiguration(2), OUTPUT_PATH_A);

    Tap inputTap = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH_A);
    Pipe pipe = getPipe("pipe");

    // Simulate that another version is present
    fs.mkdirs(new Path(OUTPUT_DIR + "/" + DOMAIN_A_NAME + "/0/other"));
    fs.mkdirs(new Path(OUTPUT_DIR + "/" + DOMAIN_A_NAME + "/1/other"));

    new CascadingDomainBuilder(properties, pipe, "key", "value").build(new Properties(), inputTap);

    // Check output
    String p1 = getContents(fs, HDFSOutputStreamFactory.getPath(OUTPUT_PATH_A, 0, "0.base"));
    String p2 = getContents(fs, HDFSOutputStreamFactory.getPath(OUTPUT_PATH_A, 1, "0.base"));
    assertEquals("0 v0\n2 v2\n4 v4\n", p1);
    assertEquals("1 v1\n3 v3\n", p2);
  }

  public void testMultipleDomains() throws IOException {
    // A
    DomainBuilderProperties propertiesA = new DomainBuilderProperties(DOMAIN_A_NAME,
        false, IntStringKeyStorageEngineCoordinator.getConfiguration(2), OUTPUT_PATH_A);
    Tap inputTapA = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH_A);
    Pipe pipeA = getPipe("a");

    // B
    DomainBuilderProperties propertiesB = new DomainBuilderProperties(DOMAIN_B_NAME,
        false, IntStringKeyStorageEngineCoordinator.getConfiguration(3), OUTPUT_PATH_B);
    Tap inputTapB = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH_B);
    Pipe pipeB = getPipe("b");

    // Sources
    Map<String, Tap> sources = new HashMap<String, Tap>();
    sources.put("a", inputTapA);
    sources.put("b", inputTapB);

    // Build domains
    CascadingDomainBuilder domainA = new CascadingDomainBuilder(propertiesA, pipeA, "key", "value");
    CascadingDomainBuilder domainB = new CascadingDomainBuilder(propertiesB, pipeB, "key", "value");
    CascadingDomainBuilder.buildDomains(new Properties(), sources, domainA, domainB);

    // Check A output
    String p0A = getContents(fs, HDFSOutputStreamFactory.getPath(OUTPUT_PATH_A, 0, "0.base"));
    String p1A = getContents(fs, HDFSOutputStreamFactory.getPath(OUTPUT_PATH_A, 1, "0.base"));
    assertEquals("0 v0\n2 v2\n4 v4\n", p0A);
    assertEquals("1 v1\n3 v3\n", p1A);

    // Check B output
    String p0B = getContents(fs, HDFSOutputStreamFactory.getPath(OUTPUT_PATH_B, 0, "0.base"));
    String p1B = getContents(fs, HDFSOutputStreamFactory.getPath(OUTPUT_PATH_B, 1, "0.base"));
    String p2B = getContents(fs, HDFSOutputStreamFactory.getPath(OUTPUT_PATH_B, 2, "0.base"));
    assertEquals("12 v2\n", p0B);
    assertEquals("10 v0\n13 v3\n", p1B);
    assertEquals("11 v1\n14 v4\n", p2B);
  }
}
