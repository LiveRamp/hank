/**
 *  Copyright 2011 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.liveramp.hank.config.yaml;

import com.liveramp.hank.test.BaseTestCase;
import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.coordinator.mock.MockCoordinator;
import org.junit.Test;

import java.io.FileWriter;
import java.io.PrintWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestYamlSmartClientDaemonConfigurator extends BaseTestCase {
  private final String configPath = localTmpDir + "/config.yml";

  @Test
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
