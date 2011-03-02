package com.rapleaf.hank.config;

import java.io.FileWriter;
import java.io.PrintWriter;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.MockCoordinator;

public class TestYamlPartDaemonConfigurator extends BaseTestCase {
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
      new YamlPartDaemonConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {}

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partserv:");
    pw.println("  local_data_dirs: ");
    pw.println("    - /path/to/some/data ");
    pw.println("  service_port: 1");
    pw.println("  ring_group_name: rg1");
    pw.println("  part_daemon:");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new YamlPartDaemonConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {}

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partserv:");
    pw.println("  local_data_dirs: ");
    pw.println("    - /path/to/some/data ");
    pw.println("  service_port: 1");
    pw.println("  ring_group_name: rg1");
    pw.println("  part_daemon:");
    pw.println("    blah: blah");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new YamlPartDaemonConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {}

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partserv:");
    pw.println("  local_data_dirs: ");
    pw.println("    - /path/to/some/data ");
    pw.println("  service_port: 1");
    pw.println("  ring_group_name: rg1");
    pw.println("  part_daemon:");
    pw.println("    num_worker_threads: blah");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
    try {
      new YamlPartDaemonConfigurator(configPath);
      fail("should have thrown an exception");
    } catch (InvalidConfigurationException e) {}

    pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partserv:");
    pw.println("  local_data_dirs: ");
    pw.println("    - /path/to/some/data ");
    pw.println("  service_port: 1");
    pw.println("  ring_group_name: rg1");
    pw.println("  part_daemon:");
    pw.println("    num_worker_threads: 5");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();

    YamlPartDaemonConfigurator conf = new YamlPartDaemonConfigurator(configPath);
    assertEquals(5, conf.getNumThreads());
  }
}
