package com.rapleaf.hank.config;

import java.io.FileWriter;
import java.io.PrintWriter;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.MockCoordinator;

public class TestYamlUpdateDaemonConfigurator extends BaseTestCase {
  private final String configPath = localTmpDir + "/config.yml";

  public void testIt() throws Exception {
    PrintWriter pw = new PrintWriter(new FileWriter(configPath));
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
    try {
      new YamlUpdateDaemonConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {}

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partserv:");
    pw.println("  local_data_dirs: ");
    pw.println("    - /path/to/some/data ");
    pw.println("  service_port: 1");
    pw.println("  ring_group_name: rg1");
    pw.println("  update_daemon:");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new YamlUpdateDaemonConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {}

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partserv:");
    pw.println("  local_data_dirs: ");
    pw.println("    - /path/to/some/data ");
    pw.println("  service_port: 1");
    pw.println("  ring_group_name: rg1");
    pw.println("  update_daemon:");
    pw.println("    blah: blah");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new YamlUpdateDaemonConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {}

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partserv:");
    pw.println("  local_data_dirs: ");
    pw.println("    - /path/to/some/data ");
    pw.println("  service_port: 1");
    pw.println("  ring_group_name: rg1");
    pw.println("  update_daemon:");
    pw.println("    num_concurrent_updates: blah");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new YamlUpdateDaemonConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {}

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partserv:");
    pw.println("  local_data_dirs: ");
    pw.println("    - /path/to/some/data ");
    pw.println("  service_port: 1");
    pw.println("  ring_group_name: rg1");
    pw.println("  update_daemon:");
    pw.println("    num_concurrent_updates: 5");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();

    YamlUpdateDaemonConfigurator conf = new YamlUpdateDaemonConfigurator(configPath);
    assertEquals(5, conf.getNumConcurrentUpdates());
  }
}
