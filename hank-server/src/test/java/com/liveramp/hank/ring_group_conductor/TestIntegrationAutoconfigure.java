package com.liveramp.hank.ring_group_conductor;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.log4j.Level;
import org.junit.Test;

import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.PartitionServerConfigurator;
import com.liveramp.hank.config.RingGroupConductorConfigurator;
import com.liveramp.hank.config.yaml.YamlPartitionServerConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.fixtures.ConfigFixtures;
import com.liveramp.hank.fixtures.PartitionServerRunnable;
import com.liveramp.hank.test.ZkTestCase;
import com.liveramp.hank.util.Condition;
import com.liveramp.hank.util.WaitUntil;
import com.liveramp.hank.zookeeper.ZkPath;

import static org.junit.Assert.*;

public class TestIntegrationAutoconfigure extends ZkTestCase {

  private final String domainsRoot = ZkPath.append(getRoot(), "domains");
  private final String domainGroupsRoot = ZkPath.append(getRoot(), "domain_groups");
  private final String ringGroupsRoot = ZkPath.append(getRoot(), "ring_groups");

  private final Map<PartitionServerAddress, Thread> partitionServerThreads = new HashMap<PartitionServerAddress, Thread>();
  private final Map<PartitionServerAddress, PartitionServerRunnable> partitionServerRunnables = new HashMap<PartitionServerAddress, PartitionServerRunnable>();

  @Test
  public void testAll() throws Exception {

    org.apache.log4j.Logger.getLogger("com.liveramp.hank.coordinator.zk").setLevel(Level.WARN);
    org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN);
    org.apache.log4j.Logger.getLogger("org.apache.zookeeper.server").setLevel(Level.WARN);

    // Logger.getLogger("com.liveramp.hank.partition_server").setLevel(Level.INFO);
    org.apache.log4j.Logger.getLogger("com.liveramp.hank.storage").setLevel(Level.TRACE);
    create(domainsRoot);
    create(domainGroupsRoot);
    create(ringGroupsRoot);

    Coordinator coordinator = ConfigFixtures.createCoordinator(localTmpDir, getZkClientPort(),
        domainsRoot,
        domainGroupsRoot,
        ringGroupsRoot
    );

    RingGroupConductorConfigurator configurator = ConfigFixtures.createRGCConfigurator(
        localTmpDir,
        getZkClientPort(),
        "group1",
        RingGroupConductorMode.AUTOCONFIGURE,
        "BUCKET",
        2,
        domainsRoot,
        domainGroupsRoot,
        ringGroupsRoot
    );


    RingGroupConductor conductor1 = new RingGroupConductor(configurator);

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          conductor1.run();
        } catch (IOException e) {
          // ok
        }
      }
    });

    thread.start();

    //  verify that we autocreate the domain group and ring group
    WaitUntil.orDie(() ->
        coordinator.getDomainGroup("group1") != null &&
            coordinator.getRingGroup("group1") != null
    );

    RingGroup ringGroup = coordinator.getRingGroup("group1");

    List<Thread> servers = Lists.newArrayList();
    for (int i = 0; i < 4; i++) {
      PartitionServerRunnable server = createServer(23456 + i, Integer.toString(i % 2));
      Thread serverThead = new Thread(server);
      serverThead.start();
      servers.add(serverThead);
    }

    //  partition servers should register alive
    WaitUntil.orDie(() -> {
      try {
        return ringGroup.getLiveServers().size() == 4;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    //  RGC should pick that up and create some rings for them
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return ringGroup.getRings().size() == 2 &&
              ringGroup.getRing(0).getHosts().size() == 2;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    for (Ring ring : ringGroup.getRings()) {
      for (Host host : ring.getHosts()) {
        WaitUntil.orDie(new Condition() {
          @Override
          public boolean test() {
            try {
              return host.getState() == HostState.SERVING;
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
      }
    }

    Multimap<Integer, Integer> hostsByRing = HashMultimap.create();

    hostsByRing.put(0, 23456);
    hostsByRing.put(0, 23458);
    hostsByRing.put(1, 23457);
    hostsByRing.put(1, 23459);

    assertEquals(hostsByRing, getHostRings(ringGroup));

    conductor1.stop();
  }

  private Multimap<Integer, Integer> getHostRings(RingGroup ringGroup) {
    Multimap<Integer, Integer> hostsByRing = HashMultimap.create();

    for (Ring ring : ringGroup.getRings()) {
      for (Host host : ring.getHosts()) {
        hostsByRing.put(ring.getRingNumber(), host.getAddress().getPortNumber());
      }
    }
    return hostsByRing;
  }

  private PartitionServerConfigurator getConfigurator(int num, String configYaml, Map<String, String> environmentFlags) throws IOException, InvalidConfigurationException {

    String configPath = localTmpDir + "/server-" + num + ".yaml";

    PrintWriter pw = new PrintWriter(new FileWriter(configPath));
    pw.write(configYaml);
    pw.close();

    return new YamlPartitionServerConfigurator(configPath) {
      @Override
      public Map<String, String> getEnvironmentFlags() {
        return environmentFlags;
      }
    };
  }

  public PartitionServerRunnable createServer(int port, String bucket) throws Exception {

    PartitionServerRunnable server = new PartitionServerRunnable(
        getConfigurator(port, ConfigFixtures.partitionServerConfig(
            null,
            "group1",
            getZkClientPort(),
            new PartitionServerAddress("localhost", port),
            domainsRoot,
            domainGroupsRoot,
            ringGroupsRoot
            ),
            Collections.singletonMap("BUCKET", bucket)
        )
    );

    return server;
  }

}