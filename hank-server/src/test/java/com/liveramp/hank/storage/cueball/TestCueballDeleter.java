package com.liveramp.hank.storage.cueball;

import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestCueballDeleter extends AbstractCueballTest {
  private final String LOCAL_ROOT = localTmpDir + "/local";

  @Before
  public void setUp() throws Exception {
    new File(LOCAL_ROOT).mkdirs();
  }

  @Test
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
