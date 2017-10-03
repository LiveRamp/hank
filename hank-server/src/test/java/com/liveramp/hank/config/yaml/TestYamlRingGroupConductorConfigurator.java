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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import com.liveramp.commons.collections.map.MapBuilder;
import com.liveramp.hank.config.RingGroupConfiguredDomain;
import com.liveramp.hank.coordinator.mock.MockCoordinator;
import com.liveramp.hank.ring_group_conductor.RingGroupConductorMode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestYamlRingGroupConductorConfigurator {

  private static final String PATH = "/tmp/yaml_ring_group_conductor_configurator.yml";

  @Test
  public void testWithDomains() throws Exception {

    new File(PATH).delete();
    PrintWriter pw = new PrintWriter(new FileWriter(PATH));
    pw.println("---");
    pw.println("ring_group_conductor:");
    pw.println("  ring_group_name: myRingGroup");
    pw.println("  sleep_interval: 1000");
    pw.println("  min_ring_fully_serving_observations: 10");
    pw.println("  initial_mode: INACTIVE");
    pw.println("  domains:");
    pw.println("   - name: domain1");
    pw.println("     partitions: 2");
    pw.println("     required_host_flags:");
    pw.println("      - flag1");
    pw.println("     storage_engine_factory: mock");
    pw.println("     storage_engine_factory_options:");
    pw.println("       option1: value1");
    pw.println("       option2: value2");
    pw.println("   - name: domain2");
    pw.println("     partitions: 2");
    pw.println("     required_host_flags:");
    pw.println("     storage_engine_factory: mock");
    pw.println("     storage_engine_factory_options:");
    pw.println("       option1: value1");
    pw.println("       option2: value2");

    pw.println("coordinator:");
    pw.println("  factory: " + MockCoordinator.Factory.class.getName());
    pw.println("  options:");
    pw.println("    blah: blah");
    pw.close();

    YamlRingGroupConductorConfigurator c = new YamlRingGroupConductorConfigurator(PATH);

    List<RingGroupConfiguredDomain> configuredDomains = c.getConfiguredDomains();
    assertEquals(2, configuredDomains.size());

    assertTrue(configuredDomains.contains(
        new RingGroupConfiguredDomain(
            "domain1",
            2,
            Lists.newArrayList("flag1"),
            "mock",
            "partitioner",
            MapBuilder.<String, Object>
                of("option1", "value1")
                .put("option2", "value2")
                .get()
        )
    ));

    assertTrue(configuredDomains.contains(
        new RingGroupConfiguredDomain(
            "domain2",
            2,
            Lists.newArrayList(),
            "mock",
            "partitioner",
            MapBuilder.<String, Object>
                of("option1", "value1")
                .put("option2", "value2")
                .get()
        )
    ));

  }

  @Test
  public void testWithoutDomains() throws Exception {

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

    YamlRingGroupConductorConfigurator c = new YamlRingGroupConductorConfigurator(PATH);
    assertEquals(1000, c.getSleepInterval());
    assertEquals(10, c.getMinRingFullyServingObservations());
    assertEquals("myRingGroup", c.getRingGroupName());
    assertTrue(c.createCoordinator() instanceof MockCoordinator);
    assertTrue(((MockCoordinator)c.createCoordinator()).getInitOptions().containsKey("blah"));
    assertEquals(RingGroupConductorMode.INACTIVE, c.getInitialMode());

    List<RingGroupConfiguredDomain> configuredDomains = c.getConfiguredDomains();
    assertEquals(0, configuredDomains.size());

  }
}
