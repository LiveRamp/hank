package com.liveramp.hank.storage.curly;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestCurlyDeleter extends AbstractCurlyTestBase {
  private final String LOCAL_ROOT = "/tmp/TestCurlyDeleter_local";

  @Before
  public void setUp() throws Exception {
    super.setUp();
    new File(LOCAL_ROOT).mkdirs();
  }

  @Test
  public void testDeleter() throws Exception {
    assertTrue(localFileExists(LOCAL_ROOT));
    CurlyDeleter deleter = new CurlyDeleter(LOCAL_ROOT);
    deleter.delete();
    assertFalse(localFileExists(LOCAL_ROOT));
  }

  private boolean localFileExists(String localFile) {
    return new File(localFile).exists();
  }
}
