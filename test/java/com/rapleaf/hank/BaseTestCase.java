package com.rapleaf.hank;

import java.io.File;

import junit.framework.TestCase;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.rapleaf.hank.util.FsUtils;

public class BaseTestCase extends TestCase {
  static {
    Logger.getRootLogger().setLevel(Level.ALL);
  }

  protected final String localTmpDir = System.getProperty("tmpDir", "/tmp/hank_local_file_system");

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    FsUtils.rmrf(localTmpDir);
    new File(localTmpDir).mkdirs();
    System.err.println("------ test start ------");
    System.out.println("------ test start ------");
  }
}
