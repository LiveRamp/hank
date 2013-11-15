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

package com.liveramp.hank.storage;

import com.liveramp.hank.hadoop.HadoopTestCase;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestHdfsPartitionRemoteFileOps extends HadoopTestCase {

  private String ROOT;
  private HdfsPartitionRemoteFileOps hdfsFileOps;

  public TestHdfsPartitionRemoteFileOps() throws IOException {
    super(TestHdfsPartitionRemoteFileOps.class);
  }

  @Before
  public void setUp() throws Exception {

    ROOT = TEST_DIR + "/hdfs/";

    fs.create(new Path(ROOT, "0/file1.txt")).close();
    fs.create(new Path(ROOT, "0/file2.txt")).close();
    hdfsFileOps = new HdfsPartitionRemoteFileOps(ROOT, 0);
  }

  @Test
  public void testCheckAbsolutePath() {
    try {
      new HdfsPartitionRemoteFileOps("relative_path/", 0);
      fail("Should fail");
    } catch (IOException e) {
      // Good
    }
  }

  @Test
  public void testCopyToLocal() throws Exception {
    assertFalse(new File(localTmpDir + "/file1.txt").exists());
    hdfsFileOps.copyToLocalRoot("file1.txt", localTmpDir);
    assertTrue(new File(localTmpDir + "/file1.txt").exists());
  }

  @Test
  public void testAttemptDelete() throws Exception {
    assertTrue(fs.exists(new Path(ROOT, "0/file1.txt")));
    assertTrue(hdfsFileOps.attemptDelete("file1.txt"));
    assertFalse(fs.exists(new Path(ROOT, "0/file1.txt")));
  }

  @Test
  public void testExists() throws Exception {
    assertTrue(hdfsFileOps.exists("file1.txt"));
    assertTrue(hdfsFileOps.exists("file2.txt"));
    assertTrue(hdfsFileOps.attemptDelete("file1.txt"));
    assertFalse(hdfsFileOps.exists("file1.txt"));
    assertTrue(hdfsFileOps.exists("file2.txt"));
    assertTrue(hdfsFileOps.attemptDelete("file2.txt"));
    assertFalse(hdfsFileOps.exists("file2.txt"));
  }
}
