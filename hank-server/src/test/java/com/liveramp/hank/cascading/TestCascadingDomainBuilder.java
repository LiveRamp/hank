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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.liveramp.cascading_ext.flow.NoOpListener;
import com.liveramp.hank.hadoop.DomainBuilderProperties;
import com.liveramp.hank.hadoop.HadoopTestCase;
import com.liveramp.hank.hadoop.IntStringKeyStorageEngineCoordinator;
import com.liveramp.hank.storage.HdfsPartitionRemoteFileOps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestCascadingDomainBuilder extends HadoopTestCase {

  private final String DOMAIN_A_NAME = "a";
  private final String INPUT_PATH_A = INPUT_DIR + "/" + DOMAIN_A_NAME;
  private final String OUTPUT_PATH_A = OUTPUT_DIR + "/" + DOMAIN_A_NAME;

  private final String DOMAIN_B_NAME = "b";
  private final String INPUT_PATH_B = INPUT_DIR + "/" + DOMAIN_B_NAME;
  private final String OUTPUT_PATH_B = OUTPUT_DIR + "/" + DOMAIN_B_NAME;

  private final String DOMAIN_C_NAME = "c";
  private final String INPUT_PATH_C = INPUT_DIR + "/" + DOMAIN_C_NAME;
  private final String OUTPUT_PATH_C = OUTPUT_DIR + "/" + DOMAIN_C_NAME;

  public TestCascadingDomainBuilder() throws IOException {
    super(TestCascadingDomainBuilder.class);
  }

  private void writeSequenceFile(String path, Fields fields, Tuple... tuples) throws IOException {
    Tap tap = new Hfs(new SequenceFile(fields), path);
    TupleEntryCollector coll = tap.openForWrite(new HadoopFlowProcess(new JobConf()));
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
    // C is empty
    writeSequenceFile(INPUT_PATH_C, new Fields("key", "value"));
  }

  private void createSortedInputs() throws IOException {
    // A
    writeSequenceFile(INPUT_PATH_A, new Fields("key", "value"),
        getTT("0", "v0"),
        getTT("2", "v2"),
        getTT("4", "v4"),
        getTT("1", "v1"),
        getTT("3", "v3"),
        getTT("5", "v5"));
  }

  private void createEmptyInputs() throws IOException {
    // A
    writeSequenceFile(INPUT_PATH_A, new Fields("key", "value"));
  }

  private void createPartiallyEmptyInputs() throws IOException {
    // A
    writeSequenceFile(INPUT_PATH_A, new Fields("key", "value"),
        getTT("1", "v1"),
        getTT("3", "v3"),
        getTT("5", "v5"));
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

  @Test
  public void testMain() throws IOException {
    createInputs();
    DomainBuilderProperties properties = new DomainBuilderProperties(DOMAIN_A_NAME,
        IntStringKeyStorageEngineCoordinator.getConfigurator(2)).setOutputPath(OUTPUT_PATH_A);

    Tap inputTap = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH_A);
    Pipe pipe = getPipe("pipe");

    // Simulate that another version is present
    fs.mkdirs(new Path(OUTPUT_DIR + "/" + DOMAIN_A_NAME + "/0/other"));
    fs.mkdirs(new Path(OUTPUT_DIR + "/" + DOMAIN_A_NAME + "/1/other"));

    new CascadingDomainBuilder(properties, null, pipe, "key", "value")
        .build(new NoOpListener(), new Properties(), "pipe", inputTap);

    // Check output
    String p1 = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_A, 0, "0.base"));
    String p2 = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_A, 1, "0.base"));
    assertEquals("0 v0\n2 v2\n4 v4\n", p1);
    assertEquals("1 v1\n3 v3\n", p2);
  }

  @Test
  public void testMultipleDomains() throws IOException {
    createInputs();
    // A
    DomainBuilderProperties propertiesA = new DomainBuilderProperties(DOMAIN_A_NAME,
        IntStringKeyStorageEngineCoordinator.getConfigurator(2)).setOutputPath(OUTPUT_PATH_A);
    Tap inputTapA = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH_A);
    Pipe pipeA = getPipe("a");

    // B
    DomainBuilderProperties propertiesB = new DomainBuilderProperties(DOMAIN_B_NAME,
        IntStringKeyStorageEngineCoordinator.getConfigurator(3)).setOutputPath(OUTPUT_PATH_B);
    Tap inputTapB = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH_B);
    Pipe pipeB = getPipe("b");

    // C
    DomainBuilderProperties propertiesC = new DomainBuilderProperties(DOMAIN_C_NAME,
        IntStringKeyStorageEngineCoordinator.getConfigurator(3)).setOutputPath(OUTPUT_PATH_C);
    Tap inputTapC = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH_C);
    Pipe pipeC = getPipe("c");

    // Sources
    Map<String, Tap> sources = new HashMap<String, Tap>();
    sources.put("a", inputTapA);
    sources.put("b", inputTapB);
    sources.put("c", inputTapC);

    // Build domains
    CascadingDomainBuilder domainA = new CascadingDomainBuilder(propertiesA, null, pipeA, "key", "value");
    CascadingDomainBuilder domainB = new CascadingDomainBuilder(propertiesB, null, pipeB, "key", "value");
    CascadingDomainBuilder domainC = new CascadingDomainBuilder(propertiesC, null, pipeC, "key", "value");
    CascadingDomainBuilder.buildDomains(new Properties(),
        sources, domainA, domainB, domainC);

    // Check A output
    String p0A = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_A, 0, "0.base"));
    String p1A = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_A, 1, "0.base"));
    assertEquals("0 v0\n2 v2\n4 v4\n", p0A);
    assertEquals("1 v1\n3 v3\n", p1A);

    // Check B output
    String p0B = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_B, 0, "0.base"));
    String p1B = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_B, 1, "0.base"));
    String p2B = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_B, 2, "0.base"));
    assertEquals("12 v2\n", p0B);
    assertEquals("10 v0\n13 v3\n", p1B);
    assertEquals("11 v1\n14 v4\n", p2B);

    // Check C output
    String p0C = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_C, 0, "0.base"));
    String p1C = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_C, 1, "0.base"));
    String p2C = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_C, 2, "0.base"));
    assertEquals("", p0C);
    assertEquals("", p1C);
    assertEquals("", p2C);
  }

  @Test
  public void testEmptyVersion() throws IOException {
    createInputs();
    DomainBuilderProperties properties = new DomainBuilderProperties(DOMAIN_C_NAME,
        IntStringKeyStorageEngineCoordinator.getConfigurator(2)).setOutputPath(OUTPUT_PATH_C);

    Tap inputTap = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH_C);
    Pipe pipe = getPipe("pipe");

    // Simulate that another version is present
    fs.mkdirs(new Path(OUTPUT_DIR + "/" + DOMAIN_C_NAME + "/0/other"));
    fs.mkdirs(new Path(OUTPUT_DIR + "/" + DOMAIN_C_NAME + "/1/other"));

    new CascadingDomainBuilder(properties, null, pipe, "key", "value")
        .build(new NoOpListener(), new Properties(), "pipe", inputTap);

    // Check output
    String p1 = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_C, 0, "0.base"));
    String p2 = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_C, 1, "0.base"));
    assertEquals("", p1);
    assertEquals("", p2);
  }

  @Test
  public void testAlreadyPartitionedAndSorted() throws IOException {
    createSortedInputs();
    DomainBuilderProperties properties = new DomainBuilderProperties(DOMAIN_A_NAME,
        IntStringKeyStorageEngineCoordinator.getConfigurator(2))
        .setOutputPath(OUTPUT_PATH_A)
        .setShouldPartitionAndSortInput(false);

    Tap inputTap = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH_A);
    Pipe pipe = getPipe("pipe");

    new CascadingDomainBuilder(properties, null, pipe, "key", "value")
        .build(new NoOpListener(), new Properties(), "pipe", inputTap);

    // Check output
    String p1 = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_A, 0, "0.base"));
    String p2 = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_A, 1, "0.base"));
    assertEquals("0 v0\n2 v2\n4 v4\n", p1);
    assertEquals("1 v1\n3 v3\n5 v5\n", p2);
  }

  @Test
  public void testAlreadyPartitionedAndSortedFailure() throws IOException {
    // Create input not sorted
    createInputs();
    DomainBuilderProperties properties = new DomainBuilderProperties(DOMAIN_A_NAME,
        IntStringKeyStorageEngineCoordinator.getConfigurator(2))
        .setOutputPath(OUTPUT_PATH_A)
            // Expect sorted input
        .setShouldPartitionAndSortInput(false);

    Tap inputTap = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH_A);
    Pipe pipe = getPipe("pipe");

    try {
      new CascadingDomainBuilder(properties, null, pipe, "key", "value")
          .build(new NoOpListener(), new Properties(), "pipe", inputTap);
      fail("Should have failed because input is not sorted");
    } catch (Exception e) {
      // Correct behavior
    }
  }

  @Test
  public void testMissingPartitions() throws IOException {
    // Create empty inputs
    createEmptyInputs();
    DomainBuilderProperties properties = new DomainBuilderProperties(DOMAIN_A_NAME,
        IntStringKeyStorageEngineCoordinator.getConfigurator(2))
        .setOutputPath(OUTPUT_PATH_A)
        .setShouldPartitionAndSortInput(true);

    Tap inputTap = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH_A);
    Pipe pipe = getPipe("pipe");

    new CascadingDomainBuilder(properties, null, pipe, "key", "value")
        .build(new NoOpListener(), new Properties(), "pipe", inputTap);

    // Check output
    String p1 = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_A, 0, "0.base"));
    String p2 = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_A, 1, "0.base"));
    assertEquals("", p1);
    assertEquals("", p2);
  }

  @Test
  public void testMissingPartitionsWithSortedInput() throws IOException {
    // Create empty inputs
    createEmptyInputs();
    DomainBuilderProperties properties = new DomainBuilderProperties(DOMAIN_A_NAME,
        IntStringKeyStorageEngineCoordinator.getConfigurator(2))
        .setOutputPath(OUTPUT_PATH_A)
        .setShouldPartitionAndSortInput(false);

    Tap inputTap = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH_A);
    Pipe pipe = getPipe("pipe");

    new CascadingDomainBuilder(properties, null, pipe, "key", "value")
        .build(new NoOpListener(), new Properties(), "pipe", inputTap);

    // Check output
    String p1 = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_A, 0, "0.base"));
    String p2 = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_A, 1, "0.base"));
    assertEquals("", p1);
    assertEquals("", p2);
  }

  @Test
  public void testPartiallyMissingPartitionsWithSortedInput() throws IOException {
    // Create partially empty inputs
    createPartiallyEmptyInputs();
    DomainBuilderProperties properties = new DomainBuilderProperties(DOMAIN_A_NAME,
        IntStringKeyStorageEngineCoordinator.getConfigurator(2))
        .setOutputPath(OUTPUT_PATH_A)
        .setShouldPartitionAndSortInput(false);

    Tap inputTap = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT_PATH_A);
    Pipe pipe = getPipe("pipe");

    new CascadingDomainBuilder(properties, null, pipe, "key", "value")
        .build(new NoOpListener(), new Properties(), "pipe", inputTap);

    // Check output
    String p1 = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_A, 0, "0.base"));
    String p2 = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_A, 1, "0.base"));
    assertEquals("", p1);
    assertEquals("1 v1\n3 v3\n5 v5\n", p2);
  }
}
