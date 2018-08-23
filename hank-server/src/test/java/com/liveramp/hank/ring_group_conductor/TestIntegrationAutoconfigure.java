package com.liveramp.hank.ring_group_conductor;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.log4j.Level;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import com.liveramp.commons.collections.map.MapBuilder;
import com.liveramp.commons.test.Condition;
import com.liveramp.commons.test.WaitUntil;
import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.PartitionServerConfigurator;
import com.liveramp.hank.config.RingGroupConfiguredDomain;
import com.liveramp.hank.config.yaml.YamlPartitionServerConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainAndVersion;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.test.ConfigFixtures;
import com.liveramp.hank.fixtures.PartitionServerRunnable;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.liveramp.hank.test.CoreConfigFixtures;
import com.liveramp.hank.test.ZkTestCase;
import com.liveramp.hank.zookeeper.ZkPath;

import static com.liveramp.hank.coordinator.zk.ZkRingGroup.RING_GROUP_CONDUCTOR_ONLINE_PATH;
import static org.junit.Assert.*;

public class TestIntegrationAutoconfigure extends ZkTestCase {

  private final String domainsRoot = ZkPath.append(getRoot(), "domains");
  private final String domainGroupsRoot = ZkPath.append(getRoot(), "domain_groups");
  private final String ringGroupsRoot = ZkPath.append(getRoot(), "ring_groups");

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

    Coordinator coordinator = CoreConfigFixtures.createCoordinator(localTmpDir, getZkClientPort(),
        1000000,
        domainsRoot,
        domainGroupsRoot,
        ringGroupsRoot
    );

    RingGroupConductor conductor1 = new RingGroupConductor(ConfigFixtures.createRGCConfigurator(
        localTmpDir,
        getZkClientPort(),
        "group1",
        RingGroupConductorMode.AUTOCONFIGURE,
        "BUCKET",
        2,
        domainsRoot,
        domainGroupsRoot,
        ringGroupsRoot,
        Lists.newArrayList(new RingGroupConfiguredDomain(
            "domain1",
            2,
            Lists.newArrayList(),
            "storage_engine",
            "partitioner",
            MapBuilder.<String, Object>of("key1","val1").get()
        ))
    ));

    Thread thread = new Thread(() -> {
      try {
        conductor1.run();
      } catch (IOException e) {
        // ok
      }
    });

    thread.start();

    //  verify that we autocreate the domain group and ring group
    WaitUntil.orDie(() ->
        coordinator.getDomainGroup("group1") != null &&
            coordinator.getRingGroup("group1") != null
    );

    //  verify we autocreated the domain
    WaitUntil.orDie(() -> {
      Domain domain = coordinator.getDomain("domain1");
      return domain != null && domain.getNumParts() == 2;
    });

    DomainGroup domainGroup = coordinator.getDomainGroup("group1");
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
    WaitUntil.orDie(() -> {
      try {
        return ringGroup.getRings().size() == 2 &&
            ringGroup.getRing(0).getHosts().size() == 2;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    for (Ring ring : ringGroup.getRings()) {
      for (Host host : ring.getHosts()) {
        WaitUntil.orDie(() -> {
          try {
            return host.getState() == HostState.SERVING;
          } catch (IOException e) {
            throw new RuntimeException(e);
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


    //  confirm that completing a version for the domain causes it to be added to the ring group

    Domain domain = coordinator.getDomain("domain1");
    DomainVersion version = domain.openNewVersion(new IncrementalDomainVersionProperties.Base());
    version.close();

    WaitUntil.orDie(() -> {
      try {
        DomainAndVersion version1 = domainGroup.getDomainVersion(domain);
        return version1 != null && version1.getVersionNumber() == 0;
      } catch (IOException e) {
        //  whatev
      }
      return false;
    });

    stopConductor(conductor1);

    //  restart conductor with new domain config

    RingGroupConductor conductor2 = new RingGroupConductor(ConfigFixtures.createRGCConfigurator(
        localTmpDir,
        getZkClientPort(),
        "group1",
        RingGroupConductorMode.AUTOCONFIGURE,
        "BUCKET",
        2,
        domainsRoot,
        domainGroupsRoot,
        ringGroupsRoot,
        Lists.newArrayList(new RingGroupConfiguredDomain(
            "domain1",
            2,
            Lists.newArrayList(),
            "storage_engine2",
            "partitioner",
            MapBuilder.<String, Object>of("key1","val1").get()
        ))
    ));

    thread = new Thread(() -> {
      try {
        conductor2.run();
      } catch (IOException e) {
        // ok
      }
    });

    thread.start();

    WaitUntil.orDie(() -> domain.getStorageEngineFactoryClassName().equals("storage_engine2"));

    stopConductor(conductor2);

    //  new target of 1 host per ring
    RingGroupConductor conductor3 = new RingGroupConductor(ConfigFixtures.createRGCConfigurator(
        localTmpDir,
        getZkClientPort(),
        "group1",
        RingGroupConductorMode.AUTOCONFIGURE,
        "BUCKET",
        1,
        domainsRoot,
        domainGroupsRoot,
        ringGroupsRoot,
        Lists.newArrayList(new RingGroupConfiguredDomain(
            "domain1",
            2,
            Lists.newArrayList(),
            "storage_engine",
            "partitioner",
            MapBuilder.<String, Object>of("key1","val1").get()
        ))
    ));

    assertEquals(2, ringGroup.getRings().size());

    thread = new Thread(() -> {
      try {
        conductor3.run();
      } catch (IOException e) {
        // ok
      }
    });
    thread.start();

    Thread.sleep(5000);

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return ringGroup.getRings().size() == 3;
      }
    });

    stopConductor(conductor3);

    //  ideally we would wait for hosts to update so we would get 4 rings,
    //  but that would require setting up a lot of root directories and stuff

  }

  private void stopConductor(RingGroupConductor conductor1) throws InterruptedException {
    conductor1.stop();

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return getZk().exists(ringGroupsRoot+"/group1/"+RING_GROUP_CONDUCTOR_ONLINE_PATH, false) == null;
        } catch (KeeperException e) {
          // eh
        } catch (InterruptedException e) {
          // eh
        }
        return true;
      }
    });
  }

  //  TODO integration test autoconfigure.
  //    -- when removing excess hosts, make sure remaining hosts are reassigned new partitions.


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