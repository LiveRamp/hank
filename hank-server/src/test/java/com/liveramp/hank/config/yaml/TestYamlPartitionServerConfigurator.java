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

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import com.liveramp.hank.coordinator.mock.MockCoordinator;
import com.liveramp.hank.test.BaseTestCase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestYamlPartitionServerConfigurator extends BaseTestCase {
  private final String configPath = localTmpDir + "/config.yml";

  @Test
  public void testIt() throws Exception {
    PrintWriter pw = new PrintWriter(new FileWriter(configPath));
    pw.println("partition_server:");
    pw.println("  local_data_dirs: ");
    pw.println("    - /path/to/some/data ");
    pw.println("  service_port: 1");
    pw.println("  ring_group_name: rg1");
    pw.println("  partition_server_daemon:");
    pw.println("    num_concurrent_queries: 5");
    pw.println("    num_concurrent_get_bulk_tasks: 1");
    pw.println("    get_bulk_task_size: 2");
    pw.println("    get_timer_aggregator_window: 1000");
    pw.println("    buffer_reuse_max_size: 1024");
    pw.println("    cache_num_bytes_capacity: 1000000");
    pw.println("    cache_num_items_capacity: 2000");
    pw.println("  update_daemon:");
    pw.println("    num_concurrent_updates: 5");
    pw.println("    max_concurrent_updates_per_data_directory: 2");
    pw.println("  environment_flags:");
    pw.println("    - PATH");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();

    YamlPartitionServerConfigurator conf = new YamlPartitionServerConfigurator(configPath);
    assertEquals(Collections.singleton("/path/to/some/data"), conf.getDataDirectories());
    assertEquals(1, conf.getServicePort());
    assertEquals("rg1", conf.getRingGroupName());
    assertEquals(5, conf.getNumConcurrentUpdates());
    assertEquals(2, conf.getMaxConcurrentUpdatesPerDataDirectory());
    assertEquals(5, conf.getNumConcurrentQueries());
    assertEquals(1024, conf.getBufferReuseMaxSize());
    assertEquals(1000000, conf.getCacheNumBytesCapacity());
    assertEquals(2000, conf.getCacheNumItemsCapacity());

    Map<String, String> flags = conf.getEnvironmentFlags();
    assertNotEquals("", flags.get("PATH"));
    assertNotEquals(null, flags.get("PATH"));

  }


}
