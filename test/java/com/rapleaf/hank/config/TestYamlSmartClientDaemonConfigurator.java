package com.rapleaf.hank.config;

import java.io.FileWriter;
import java.io.PrintWriter;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.MockCoordinator;

public class TestYamlSmartClientDaemonConfigurator extends BaseTestCase {
  private final String configPath = localTmpDir + "/config.yml";

  public void testIt() throws Exception {
    PrintWriter pw = new PrintWriter(new FileWriter(configPath));
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new YamlSmartClientDaemonConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {}

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("smart_client:");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new YamlSmartClientDaemonConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {}

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("smart_client:");
    pw.println("  blah: blah");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new YamlSmartClientDaemonConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {}

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("smart_client:");
    pw.println("  service_port: blah");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new YamlSmartClientDaemonConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {}
    
    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("smart_client:");
    pw.println("  service_port: 1");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new YamlSmartClientDaemonConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {}

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("smart_client:");
    pw.println("  service_port: 1");
    pw.println("  num_worker_threads: blah");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new YamlSmartClientDaemonConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {}

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("smart_client:");
    pw.println("  service_port: 1");
    pw.println("  num_worker_threads: 1");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new YamlSmartClientDaemonConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {}

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("smart_client:");
    pw.println("  service_port: 1");
    pw.println("  num_worker_threads: 1");
    pw.println("  ring_group_name: rg1");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    YamlSmartClientDaemonConfigurator conf = new YamlSmartClientDaemonConfigurator(configPath);
    assertEquals("rg1", conf.getRingGroupName());
    assertEquals(1, conf.getPortNumber());
    assertEquals(1, conf.getNumThreads());
  }
}
