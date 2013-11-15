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
package com.liveramp.hank.hadoop;

import com.liveramp.hank.storage.HdfsPartitionRemoteFileOps;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class TestHadoopDomainBuilder extends HadoopTestCase {

  private final String DOMAIN_A_NAME = "a";
  private final String INPUT_PATH_A = INPUT_DIR + "/" + DOMAIN_A_NAME;
  private final String OUTPUT_PATH_A = OUTPUT_DIR + "/" + DOMAIN_A_NAME;
  private final String DOMAIN_B_NAME = "b";
  private final String INPUT_PATH_B = INPUT_DIR + "/" + DOMAIN_B_NAME;
  private final String OUTPUT_PATH_B = OUTPUT_DIR + "/" + DOMAIN_B_NAME;

  public TestHadoopDomainBuilder() throws IOException {
    super(TestHadoopDomainBuilder.class);
  }

  @Before
  public void setUp() throws Exception {
    // Create inputs
    outputFile(fs, INPUT_PATH_A, "0 v0\n1 v1\n2 v2\n3 v3\n4 v4");
    outputFile(fs, INPUT_PATH_B, "4 v4\n1 v1\n2 v2\n0 v0\n3 v3");
  }

  @Test
  public void testFailIfOutputExists() throws IOException {
    fs.create(new Path(OUTPUT_PATH_A));
    try {
      new HadoopDomainBuilder(INPUT_PATH_A, TextInputFormat.class, TestMapper.class)
          .buildHankDomain(new DomainBuilderProperties(DOMAIN_A_NAME,
              IntStringKeyStorageEngineCoordinator.getConfigurator(2)).setOutputPath(OUTPUT_PATH_A), null);
      fail("Should fail when output exists");
    } catch (IOException e) {
    }
  }

  @Test
  public void testOutput() throws IOException {
    new HadoopDomainBuilder(INPUT_PATH_A, TextInputFormat.class, TestMapper.class)
        .buildHankDomain(new DomainBuilderProperties(DOMAIN_A_NAME,
            IntStringKeyStorageEngineCoordinator.getConfigurator(2)).setOutputPath(OUTPUT_PATH_A), null);
    String p1 = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_A, 0, "0.base"));
    String p2 = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_A, 1, "0.base"));
    assertEquals("0 v0\n2 v2\n4 v4\n", p1);
    assertEquals("1 v1\n3 v3\n", p2);
  }

  @Test
  public void testSorted() throws IOException {
    new HadoopDomainBuilder(INPUT_PATH_B, TextInputFormat.class, TestMapper.class)
        .buildHankDomain(new DomainBuilderProperties(DOMAIN_B_NAME,
            IntStringKeyStorageEngineCoordinator.getConfigurator(2)).setOutputPath(OUTPUT_PATH_B), null);
    String p1 = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_B, 0, "0.base"));
    String p2 = getContents(fs, HdfsPartitionRemoteFileOps.getRemoteAbsolutePath(OUTPUT_PATH_B, 1, "0.base"));
    assertEquals("0 v0\n2 v2\n4 v4\n", p1);
    assertEquals("1 v1\n3 v3\n", p2);
  }

  public static class TestMapper extends DomainBuilderMapper<LongWritable, Text> {

    // Converts text file lines "<key> <value>" to the corresponding object
    @Override
    protected KeyValuePair buildHankKeyValue(LongWritable key, Text value) {
      String[] splits = value.toString().split(" ");
      if (splits.length != 2) {
        throw new RuntimeException("Input text file must be lines like \"<key> <value>\"");
      }
      return new KeyValuePair(splits[0].getBytes(), splits[1].getBytes());
    }
  }
}
