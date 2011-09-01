/**
 *  Copyright 2011 Rapleaf
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
package com.rapleaf.hank.config.yaml;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Collections;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.mock.MockCoordinator;

public class TestYamlPartitionServerConfigurator extends BaseTestCase {
  private final String configPath = localTmpDir + "/config.yml";

  public void testIt() throws Exception {
    PrintWriter pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partition_server:");
    pw.println("  local_data_dirs: ");
    pw.println("    - /path/to/some/data ");
    pw.println("  service_port: 1");
    pw.println("  ring_group_name: rg1");
    pw.println("  partition_server_daemon:");
    pw.println("    num_worker_threads: 5");
    pw.println("  update_daemon:");
    pw.println("    num_concurrent_updates: 5");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();

    YamlPartitionServerConfigurator conf = new YamlPartitionServerConfigurator(configPath);
    assertEquals(Collections.singleton("/path/to/some/data"), conf.getLocalDataDirectories());
    assertEquals(1, conf.getServicePort());
    assertEquals("rg1", conf.getRingGroupName());
    assertEquals(5, conf.getNumConcurrentUpdates());
    assertEquals(5, conf.getNumThreads());
  }
}
