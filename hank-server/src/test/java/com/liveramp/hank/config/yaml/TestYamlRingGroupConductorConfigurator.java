/**
 * Copyright 2011 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.liveramp.hank.config.yaml;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import org.junit.Before;
import org.junit.Test;

import com.liveramp.hank.coordinator.mock.MockCoordinator;
import com.liveramp.hank.ring_group_conductor.RingGroupConductorMode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestYamlRingGroupConductorConfigurator {

  private static final String PATH = "/tmp/yaml_ring_group_conductor_configurator.yml";

  @Before
  public void setUp() throws Exception {
    new File(PATH).delete();
    PrintWriter pw = new PrintWriter(new FileWriter(PATH));
    pw.println("---");
    pw.println("ring_group_conductor:");
    pw.println("  ring_group_name: myRingGroup");
    pw.println("  sleep_interval: 1000");
    pw.println("  min_ring_fully_serving_observations: 10");
    pw.println("  initial_mode: INACTIVE");
    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();
  }

  @Test
  public void testIt() throws Exception {
    YamlRingGroupConductorConfigurator c = new YamlRingGroupConductorConfigurator(PATH);
    assertEquals(1000, c.getSleepInterval());
    assertEquals(10, c.getMinRingFullyServingObservations());
    assertEquals("myRingGroup", c.getRingGroupName());
    assertTrue(c.createCoordinator() instanceof MockCoordinator);
    assertTrue(((MockCoordinator)c.createCoordinator()).getInitOptions().containsKey("blah"));
    assertEquals(RingGroupConductorMode.INACTIVE, c.getInitialMode());
  }
}
