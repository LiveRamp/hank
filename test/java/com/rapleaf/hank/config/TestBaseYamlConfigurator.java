package com.rapleaf.hank.config;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.MockCoordinator;

public class TestBaseYamlConfigurator extends BaseTestCase {
  private final String configPath = localTmpDir + "/bad4.yml";

  private static class TestImplOfBaseYamlConfigurator extends BaseYamlConfigurator {
    public TestImplOfBaseYamlConfigurator(String path) throws IOException,
    InvalidConfigurationException {
      super(path);
    }
  }

  public void testIt() throws Exception {
    PrintWriter pw = new PrintWriter(new FileWriter(configPath));
    pw.println("cordinator:");
    pw.close();
    try {
      new TestImplOfBaseYamlConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {
      //yay!
    }

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("coordinator:");
    pw.close();
    try {
      new TestImplOfBaseYamlConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {
      //yay!
    }

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("coordinator:");
    pw.println("  blah: blah");
    pw.close();
    try {
      new TestImplOfBaseYamlConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {
      //yay!
    }

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("coordinator:");
    pw.println("  factory: 1");
    pw.close();
    try {
      new TestImplOfBaseYamlConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {
      //yay!
    }

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.close();
    try {
      new TestImplOfBaseYamlConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {
      //yay!
    }

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    TestImplOfBaseYamlConfigurator conf = new TestImplOfBaseYamlConfigurator(configPath);
    Coordinator coord = conf.getCoordinator();
    assertTrue(coord instanceof MockCoordinator);
  }
}
