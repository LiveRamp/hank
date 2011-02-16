package com.rapleaf.hank;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import junit.framework.TestCase;

public class BaseTestCase extends TestCase {
  static {
    Logger.getRootLogger().setLevel(Level.ALL);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    System.err.println("------ test start ------");
    System.out.println("------ test start ------");
  }
}
