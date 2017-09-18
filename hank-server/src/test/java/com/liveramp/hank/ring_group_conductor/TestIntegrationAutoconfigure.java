package com.liveramp.hank.ring_group_conductor;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.log4j.Level;
import org.junit.Test;

import com.liveramp.hank.config.CoordinatorConfigurator;
import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.PartitionServerConfigurator;
import com.liveramp.hank.config.yaml.YamlClientConfigurator;
import com.liveramp.hank.config.yaml.YamlPartitionServerConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.fixtures.ConfigFixtures;
import com.liveramp.hank.fixtures.PartitionServerRunnable;
import com.liveramp.hank.test.BaseTestCase;
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

    coordinator.addDomainGroup("dg1");
    RingGroup ringGroup = coordinator.addRingGroup("rg1", "dg1");

    List<Thread> servers = Lists.newArrayList();

    //  0, 1, 0, 1

    for (int i = 0; i < 4; i++) {
      PartitionServerRunnable server = createServer(23456 + i, Integer.toString(i % 2));
      Thread serverThead = new Thread(server);
      serverThead.start();
      servers.add(serverThead);
    }

    WaitUntil.orDie(() -> {
      try {
        return ringGroup.getLiveServers().size() == 4;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    RingGroupAutoconfigureTransitionFunction transition = new RingGroupAutoconfigureTransitionFunction(2, "BUCKET");
    transition.manageTransitions(ringGroup);

    assertEquals(2, ringGroup.getRings().size());
    assertEquals(2, ringGroup.getRing(0).getHosts().size());

    Set<Set<Integer>> hostSets = Sets.newHashSet();

    for (Ring ring : ringGroup.getRings()) {

      Set<Integer> hosts = Sets.newHashSet();
      for (Host host : ring.getHosts()) {
        hosts.add(host.getAddress().getPortNumber());
        WaitUntil.orDie(new Condition() {
          @Override
          public boolean test() {
            try {
              return host.getState() == HostState.IDLE;
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
      }
      hostSets.add(hosts);
    }

    //  respect bucket
    Set<Set<Integer>> expected = Sets.newHashSet(
        Sets.newHashSet(23456, 23458),
        Sets.newHashSet(23457, 23459)
    );

    assertEquals(expected, hostSets);

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

    //  TODO get avail bucket in here
    PartitionServerRunnable server = new PartitionServerRunnable(
        getConfigurator(port, ConfigFixtures.partitionServerConfig(
            null,
            "rg1",
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

  //  TODO no-op if configured correctly

  //  TODO leftover servers

}