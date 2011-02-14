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
package com.rapleaf.hank.config;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;

import junit.framework.TestCase;

public class TestYamlConfigurator extends TestCase {
  private static final String CFG_PATH = "/tmp/hank_yml_configurator.yml";

  public void testLoad() throws Exception {
    new File(CFG_PATH).delete();
    PrintWriter bw = new PrintWriter(new BufferedWriter(new FileWriter(CFG_PATH)));
    bw.println("---");
    bw.println("ring_group_name: rapleaf-1");
    bw.println("ring_number: 15");
    bw.println("local_data_dirs:");
    bw.println("  - /tmp/hank_data_dir_1/");
    bw.println("  - /tmp/hank_data_dir_2/");
    bw.println("part_daemon:");
    bw.println("  num_threads: 3");
    bw.println("  service_port: 4545");
    bw.println("update_daemon:");
    bw.println("  num_concurrent_updates: 5");
    bw.println("coordinator:");
    bw.println("  factory: ZooKeeperCoordinator");
    bw.println("  options:");
    bw.println("    connect_string: 1.2.3.4:2181,2.3.4.5:2181,3.4.5.6:2181");
    bw.println("    session_timeout: 2010");
    bw.close();

    YamlConfigurator cfg = new YamlConfigurator(CFG_PATH);

    assertEquals(cfg.getRingGroupName(), "rapleaf-1");
    assertEquals(cfg.getRingNumber(), 15);
    assertEquals(cfg.getServicePort(), 4545);
    assertEquals(cfg.getNumThreads(), 3);
    assertEquals(cfg.getNumConcurrentUpdates(), 5);
    assertEquals(new HashSet<String>(Arrays.asList("/tmp/hank_data_dir_1/", "/tmp/hank_data_dir_2/")), cfg.getLocalDataDirectories());
  }
}
