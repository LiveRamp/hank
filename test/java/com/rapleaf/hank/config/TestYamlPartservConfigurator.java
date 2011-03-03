package com.rapleaf.hank.config;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Collections;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.MockCoordinator;

public class TestYamlPartservConfigurator extends BaseTestCase {
  private final String configPath = localTmpDir + "/config.yml";

  public void testIt() throws Exception {
    PrintWriter pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partserv:");
    pw.println("  local_data_dirs: ");
    pw.println("    - /path/to/some/data ");
    pw.println("  service_port: 1");
    pw.println("  ring_group_name: rg1");
    pw.println("  part_daemon:");
    pw.println("    num_worker_threads: 5");
    pw.println("  update_daemon:");
    pw.println("    num_concurrent_updates: 5");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();

    YamlPartservConfigurator conf = new YamlPartservConfigurator(configPath);
    assertEquals(Collections.singleton("/path/to/some/data"), conf.getLocalDataDirectories());
    assertEquals(1, conf.getServicePort());
    assertEquals("rg1", conf.getRingGroupName());
    assertEquals(5, conf.getNumConcurrentUpdates());
    assertEquals(5, conf.getNumThreads());
  }
}
