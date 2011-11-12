package com.rapleaf.hank.storage;

import com.rapleaf.hank.BaseTestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;

public class TestHdfsFileOps extends BaseTestCase {
  private static final String ROOT = "/tmp/TestHdfsFileOps";
  private FileSystem fs;

  public void testListFiles() throws Exception {
    HdfsFileOps hdfsFileOps = new HdfsFileOps(ROOT);
    assertEquals(new HashSet<String>(Arrays.asList(ROOT + "/file1.txt", ROOT + "/file2.txt")),
        new HashSet<String>(hdfsFileOps.listFiles()));
  }

  public void testCopyToLocal() throws Exception {
    assertFalse(new File(localTmpDir + "/file1.txt").exists());
    HdfsFileOps hdfsFileOps = new HdfsFileOps(ROOT);
    hdfsFileOps.copyToLocal(ROOT + "/file1.txt", localTmpDir);
    assertTrue(new File(localTmpDir + "/file1.txt").exists());
  }

  public void testAttemptDeleteRemote() throws Exception {
    HdfsFileOps hdfsFileOps = new HdfsFileOps(ROOT);
    assertTrue(fs.exists(new Path(ROOT, "file1.txt")));
    assertTrue(hdfsFileOps.attemptDeleteRemote(ROOT + "/file1.txt"));
    assertFalse(fs.exists(new Path(ROOT, "file1.txt")));

    assertFalse(fs.exists(new Path(ROOT, "file3.txt")));
    assertFalse(hdfsFileOps.attemptDeleteRemote(ROOT + "/file3.txt"));
  }

  public void testExists() throws Exception {
    HdfsFileOps hdfsFileOps = new HdfsFileOps(ROOT);
    assertTrue(hdfsFileOps.exists(ROOT + "/file1.txt"));
    assertTrue(hdfsFileOps.exists(ROOT + "/file2.txt"));
    assertTrue(hdfsFileOps.attemptDeleteRemote(ROOT + "/file1.txt"));
    assertFalse(hdfsFileOps.exists(ROOT + "/file1.txt"));
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    fs = FileSystem.get(new Configuration());
    System.out.println(fs.toString());
    fs.delete(new Path(ROOT), true);
    fs.mkdirs(new Path(ROOT));
    fs.create(new Path(ROOT, "file1.txt")).close();
    fs.create(new Path(ROOT, "file2.txt")).close();
  }
}
