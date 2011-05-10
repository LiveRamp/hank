package com.rapleaf.hank.storage.cueball;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rapleaf.hank.BaseTestCase;

public class TestHdfsFileOps extends BaseTestCase {
  private static final String ROOT = "/tmp/TestHdfsFileOps";
  private FileSystem fs;

  public void testListFiles() throws Exception {
    HdfsFileOps hdfsFileOps = new HdfsFileOps(localTmpDir, ROOT);
    assertEquals(new HashSet<String>(Arrays.asList(ROOT + "/file1.txt", ROOT + "/file2.txt")), new HashSet<String>(hdfsFileOps.listFiles()));
  }

  public void testCopyToLocal() throws Exception {
    fail();
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
