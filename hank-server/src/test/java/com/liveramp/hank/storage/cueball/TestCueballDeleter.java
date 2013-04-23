package com.liveramp.hank.storage.cueball;

import java.io.File;

public class TestCueballDeleter extends AbstractCueballTest {
  private final String LOCAL_ROOT = localTmpDir + "/local";

  public void setUp() throws Exception {
    super.setUp();
    new File(LOCAL_ROOT).mkdirs();
  }
  
  public void testDeleter() throws Exception {
    assertTrue(localFileExists(LOCAL_ROOT));
    CueballDeleter deleter = new CueballDeleter(LOCAL_ROOT);
    deleter.delete();
    assertFalse(localFileExists(LOCAL_ROOT));
  }
  
  private boolean localFileExists(String localFile) {
    return new File(localFile).exists();
  }
}
