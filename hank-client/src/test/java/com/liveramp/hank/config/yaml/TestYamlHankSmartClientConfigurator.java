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

import org.junit.Test;

import com.liveramp.hank.config.EnvironmentValue;
import com.liveramp.hank.coordinator.mock.MockCoordinator;
import com.liveramp.hank.test.BaseTestCase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestYamlHankSmartClientConfigurator extends BaseTestCase {
  private final String configPath = localTmpDir + "/config.yml";

  @Test
  public void testIt() throws Exception {
    PrintWriter pw = new PrintWriter(new FileWriter(configPath));
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.println("hank_smart_client:");
    pw.println("  ring_group_name: rg1");
    pw.println("  num_connections_per_host: 10");
    pw.println("  query_num_max_tries: 5");
    pw.println("  try_lock_connection_timeout_ms: 50");
    pw.println("  establish_connection_timeout_ms: 100");
    pw.println("  query_timeout_ms: 42");
    pw.println("  bulk_query_timeout_ms: 142");
    pw.println("  preferred_environment_key: PATH");

    pw.close();

    YamlHankSmartClientConfigurator conf = new YamlHankSmartClientConfigurator(configPath);

    assertEquals("rg1", conf.getRingGroupName());
    assertEquals(10, conf.getNumConnectionsPerHost());
    assertEquals(5, conf.getQueryNumMaxTries());
    assertEquals(50, conf.getTryLockConnectionTimeoutMs());
    assertEquals(100, conf.getEstablishConnectionTimeoutMs());
    assertEquals(42, conf.getQueryTimeoutMs());
    assertEquals(142, conf.getBulkQueryTimeoutMs());

    EnvironmentValue env = conf.getPreferredServerEnvironment();
    assertEquals("PATH", env.getKey());
    assertNotEquals(null, env.getValue());
    assertNotEquals("", env.getValue());
  }
}
