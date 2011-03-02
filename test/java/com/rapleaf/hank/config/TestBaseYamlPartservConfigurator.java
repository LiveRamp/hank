package com.rapleaf.hank.config;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.MockCoordinator;

public class TestBaseYamlPartservConfigurator extends BaseTestCase {
  private static final class TestImplOfBaseYamlPartservConfigurator
  extends BaseYamlPartservConfigurator {
    public TestImplOfBaseYamlPartservConfigurator(String path)
    throws IOException, InvalidConfigurationException {
      super(path);
    }
  }

  private final String configPath = localTmpDir + "/config.yml";

  public void testIt() throws Exception {
    PrintWriter pw = new PrintWriter(new FileWriter(configPath));
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new TestImplOfBaseYamlPartservConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {
      //yay!
    }

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partserv:");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new TestImplOfBaseYamlPartservConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {
      //yay!
    }

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partserv:");
    pw.println("  blah: blah");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new TestImplOfBaseYamlPartservConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {
      //yay!
    }

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partserv:");
    pw.println("  local_data_dirs: blah");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new TestImplOfBaseYamlPartservConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {
      //yay!
    }

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partserv:");
    pw.println("  local_data_dirs: ");
    pw.println("  - /path/to/some/data ");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new TestImplOfBaseYamlPartservConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {
      //yay!
    }

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partserv:");
    pw.println("  local_data_dirs: ");
    pw.println("    - /path/to/some/data ");
    pw.println("  service_port: blah");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new TestImplOfBaseYamlPartservConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {
      //yay!
    }

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partserv:");
    pw.println("  local_data_dirs: ");
    pw.println("    - /path/to/some/data ");
    pw.println("  service_port: 1");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new TestImplOfBaseYamlPartservConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {
      //yay!
    }

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partserv:");
    pw.println("  local_data_dirs: ");
    pw.println("    - /path/to/some/data ");
    pw.println("  service_port: 1");
    pw.println("  ring_group_name: rg1");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    TestImplOfBaseYamlPartservConfigurator conf = new TestImplOfBaseYamlPartservConfigurator(configPath);
    assertEquals(Collections.singleton("/path/to/some/data"), conf.getLocalDataDirectories());
    assertEquals(1, conf.getServicePort());
    assertEquals("rg1", conf.getRingGroupName());
  }
}
