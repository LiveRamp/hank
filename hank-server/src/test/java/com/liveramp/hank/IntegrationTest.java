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
package com.liveramp.hank;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.collect.Lists;
import org.apache.log4j.Level;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.commons.util.BytesUtils;
import com.liveramp.hank.compression.cueball.GzipCueballCompressionCodec;
import com.liveramp.hank.config.CoordinatorConfigurator;
import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.PartitionServerConfigurator;
import com.liveramp.hank.config.RingGroupConductorConfigurator;
import com.liveramp.hank.config.SmartClientDaemonConfigurator;
import com.liveramp.hank.config.yaml.YamlClientConfigurator;
import com.liveramp.hank.config.yaml.YamlPartitionServerConfigurator;
import com.liveramp.hank.config.yaml.YamlRingGroupConductorConfigurator;
import com.liveramp.hank.config.yaml.YamlSmartClientDaemonConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.Domains;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostCommand;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.coordinator.RingGroups;
import com.liveramp.hank.coordinator.Rings;
import com.liveramp.hank.fixtures.ConfigFixtures;
import com.liveramp.hank.fixtures.PartitionServerRunnable;
import com.liveramp.hank.fixtures.RingGroupConductorRunnable;
import com.liveramp.hank.generated.HankBulkResponse;
import com.liveramp.hank.generated.HankException;
import com.liveramp.hank.generated.HankResponse;
import com.liveramp.hank.generated.SmartClient;
import com.liveramp.hank.hasher.Murmur64Hasher;
import com.liveramp.hank.partitioner.Murmur64Partitioner;
import com.liveramp.hank.partitioner.Partitioner;
import com.liveramp.hank.ring_group_conductor.RingGroupConductor;
import com.liveramp.hank.ring_group_conductor.RingGroupConductorMode;
import com.liveramp.hank.storage.LocalPartitionRemoteFileOps;
import com.liveramp.hank.storage.StorageEngine;
import com.liveramp.hank.storage.Writer;
import com.liveramp.hank.storage.curly.Curly;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.liveramp.hank.test.ZkTestCase;
import com.liveramp.hank.util.Condition;
import com.liveramp.hank.util.WaitUntil;
import com.liveramp.hank.zookeeper.ZkPath;

import static com.liveramp.hank.fixtures.ConfigFixtures.coordinatorConfig;
import static org.junit.Assert.assertEquals;

public class IntegrationTest extends ZkTestCase {

  private final class SmartClientRunnable implements Runnable {

    private final String configPath;
    private com.liveramp.hank.client.SmartClientDaemon server;
    private final SmartClientDaemonConfigurator configurator;

    public SmartClientRunnable() throws Exception {
      this.configPath = localTmpDir + "/smart_client_config.yml";
      PrintWriter pw = new PrintWriter(new FileWriter(configPath));
      pw.println(YamlSmartClientDaemonConfigurator.SMART_CLIENT_SECTION_KEY + ":");
      pw.println("  " + YamlSmartClientDaemonConfigurator.SERVICE_PORT_KEY + ": 50004");
      pw.println("  " + YamlSmartClientDaemonConfigurator.NUM_WORKER_THREADS + ": 1");
      pw.println("  " + YamlSmartClientDaemonConfigurator.RING_GROUP_NAME_KEY + ": rg1");
      pw.println(coordinatorConfig(getZkClientPort(), domainsRoot, domainGroupsRoot, ringGroupsRoot));
      pw.close();
      configurator = new YamlSmartClientDaemonConfigurator(configPath);
    }

    public void run() {
      server = new com.liveramp.hank.client.SmartClientDaemon(configurator);
      server.startServer();
    }

    public void pleaseStop() {
      server.downServer();
    }
  }


  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTest.class);
  private final String DOMAIN_0_DATAFILES = localTmpDir + "/domain0_datafiles";
  private final String DOMAIN_1_DATAFILES = localTmpDir + "/domain1_datafiles";

  private final String domainsRoot = ZkPath.append(getRoot(), "domains");
  private final String domainGroupsRoot = ZkPath.append(getRoot(), "domain_groups");
  private final String ringGroupsRoot = ZkPath.append(getRoot(), "ring_groups");

  private final String clientConfigYml = localTmpDir + "/config.yml";
  private final Map<PartitionServerAddress, Thread> partitionServerThreads = new HashMap<PartitionServerAddress, Thread>();
  private final Map<PartitionServerAddress, PartitionServerRunnable> partitionServerRunnables = new HashMap<PartitionServerAddress, PartitionServerRunnable>();

  private Thread ringGroupConductorThread;
  private RingGroupConductorRunnable ringGroupConductorRunnable;
  private SmartClientRunnable smartClientRunnable;

  @Test
  public void testItAll() throws Throwable {
    org.apache.log4j.Logger.getLogger("com.liveramp.hank.coordinator.zk").setLevel(Level.WARN);
    org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN);
    org.apache.log4j.Logger.getLogger("org.apache.zookeeper.server").setLevel(Level.WARN);

    // Logger.getLogger("com.liveramp.hank.partition_server").setLevel(Level.INFO);
    org.apache.log4j.Logger.getLogger("com.liveramp.hank.storage").setLevel(Level.TRACE);
    create(domainsRoot);
    create(domainGroupsRoot);
    create(ringGroupsRoot);

    PrintWriter pw = new PrintWriter(new FileWriter(clientConfigYml));
    pw.println("coordinator:");
    pw.println("  factory: com.liveramp.hank.coordinator.zk.ZooKeeperCoordinator$Factory");
    pw.println("  options:");
    pw.println("    connect_string: localhost:" + getZkClientPort());
    pw.println("    session_timeout: 1000000");
    pw.println("    domains_root: " + domainsRoot);
    pw.println("    domain_groups_root: " + domainGroupsRoot);
    pw.println("    ring_groups_root: " + ringGroupsRoot);
    pw.println("    max_connection_attempts: 5");
    pw.close();

    CoordinatorConfigurator config = new YamlClientConfigurator(clientConfigYml);

    final Coordinator coordinator = config.createCoordinator();

    StringWriter sw = new StringWriter();
    pw = new PrintWriter(sw);
    pw.println("key_hash_size: 10");
    pw.println("hasher: " + Murmur64Hasher.class.getName());
    pw.println("max_allowed_part_size: " + 1024 * 1024);
    pw.println("hash_index_bits: 1");
    pw.println("record_file_read_buffer_bytes: 10240");
    pw.println("remote_domain_root: " + DOMAIN_0_DATAFILES);
    pw.println("file_ops_factory: " + LocalPartitionRemoteFileOps.Factory.class.getName());
    pw.println("num_remote_leaf_versions_to_keep: 0");
    pw.close();
    coordinator.addDomain("domain0", 2, Curly.Factory.class.getName(), sw.toString(), Murmur64Partitioner.class.getName(), Collections.<String>emptyList());

    sw = new StringWriter();
    pw = new PrintWriter(sw);
    pw.println("key_hash_size: 10");
    pw.println("hasher: " + Murmur64Hasher.class.getName());
    pw.println("max_allowed_part_size: " + 1024 * 1024);
    pw.println("hash_index_bits: 1");
    pw.println("record_file_read_buffer_bytes: 10240");
    pw.println("remote_domain_root: " + DOMAIN_1_DATAFILES);
    pw.println("file_ops_factory: " + LocalPartitionRemoteFileOps.Factory.class.getName());
    pw.println("compression_codec: " + GzipCueballCompressionCodec.class.getName());
    pw.println("num_remote_leaf_versions_to_keep: 0");
    pw.close();
    coordinator.addDomain("domain1", 2, Curly.Factory.class.getName(), sw.toString(), Murmur64Partitioner.class.getName(), Collections.<String>emptyList());

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return coordinator.getDomain("domain0") != null && coordinator.getDomain("domain1") != null;
      }
    });

    // create empty versions of each domain

    // write a base version of each domain
    Map<ByteBuffer, ByteBuffer> domain0DataItems = new HashMap<ByteBuffer, ByteBuffer>();
    domain0DataItems.put(bb(1), bb(1, 1));
    domain0DataItems.put(bb(2), bb(2, 2));
    domain0DataItems.put(bb(3), bb(3, 3));
    domain0DataItems.put(bb(4), bb(4, 4));
    domain0DataItems.put(bb(5), bb(5, 1));
    domain0DataItems.put(bb(6), bb(6, 2));
    domain0DataItems.put(bb(7), bb(7, 3));
    domain0DataItems.put(bb(8), bb(8, 4));

    writeOut(coordinator.getDomain("domain0"), domain0DataItems, 0, DOMAIN_0_DATAFILES);

    Map<ByteBuffer, ByteBuffer> domain1DataItems = new HashMap<ByteBuffer, ByteBuffer>();
    domain1DataItems.put(bb(4), bb(1, 1));
    domain1DataItems.put(bb(3), bb(2, 2));
    domain1DataItems.put(bb(2), bb(3, 3));
    domain1DataItems.put(bb(1), bb(4, 4));
    domain1DataItems.put(bb(8), bb(5, 1));
    domain1DataItems.put(bb(7), bb(6, 2));
    domain1DataItems.put(bb(6), bb(7, 3));
    domain1DataItems.put(bb(5), bb(8, 4));

    writeOut(coordinator.getDomain("domain1"), domain1DataItems, 0, DOMAIN_1_DATAFILES);

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return coordinator.getDomain("domain0").getVersion(0) != null
              && coordinator.getDomain("domain1").getVersion(0) != null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    // configure domain group
    coordinator.addDomainGroup("dg1");

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return coordinator.getDomainGroup("dg1") != null;
      }
    });
    final DomainGroup domainGroup = coordinator.getDomainGroup("dg1");

    Map<Domain, Integer> versionMap = new HashMap<Domain, Integer>();
    versionMap.put(coordinator.getDomain("domain0"), 0);
    versionMap.put(coordinator.getDomain("domain1"), 0);
    domainGroup.setDomainVersions(versionMap);

    // configure ring group
    final RingGroup rg1 = coordinator.addRingGroup("rg1", "dg1");

    // add ring 1
    final Ring rg1r1 = rg1.addRing(1);
    final Host r1h1 = rg1r1.addHost(PartitionServerAddress.parse("localhost:50000"), Collections.<String>emptyList());
    final Host r1h2 = rg1r1.addHost(PartitionServerAddress.parse("localhost:50001"), Collections.<String>emptyList());

    // add ring 2
    final Ring rg1r2 = rg1.addRing(2);
    final Host r2h1 = rg1r2.addHost(PartitionServerAddress.parse("localhost:50002"), Collections.<String>emptyList());
    final Host r2h2 = rg1r2.addHost(PartitionServerAddress.parse("localhost:50003"), Collections.<String>emptyList());

    // Add domains
    // Domain0
    HostDomain r1h1d0 = r1h1.addDomain(coordinator.getDomain("domain0"));
    HostDomain r1h2d0 = r1h2.addDomain(coordinator.getDomain("domain0"));
    HostDomain r2h1d0 = r2h1.addDomain(coordinator.getDomain("domain0"));
    HostDomain r2h2d0 = r2h2.addDomain(coordinator.getDomain("domain0"));
    // Domain1
    HostDomain r1h1d1 = r1h1.addDomain(coordinator.getDomain("domain1"));
    HostDomain r1h2d1 = r1h2.addDomain(coordinator.getDomain("domain1"));
    HostDomain r2h1d1 = r2h1.addDomain(coordinator.getDomain("domain1"));
    HostDomain r2h2d1 = r2h2.addDomain(coordinator.getDomain("domain1"));

    // Add partitions
    // Domain0
    r1h1d0.addPartition(0);
    r1h2d0.addPartition(1);
    r2h1d0.addPartition(0);
    r2h2d0.addPartition(1);
    // Domain1
    r1h1d1.addPartition(0);
    r1h2d1.addPartition(1);
    r2h1d1.addPartition(0);
    r2h2d1.addPartition(1);

    // launch 2x 2-node rings
    startDaemons(new PartitionServerAddress("localhost", 50000));
    startDaemons(new PartitionServerAddress("localhost", 50001));
    startDaemons(new PartitionServerAddress("localhost", 50002));
    startDaemons(new PartitionServerAddress("localhost", 50003));

    // launch the Ring Group Conductor
    startRingGroupConductor();

    // Wait for update to finish
    waitForRingGroupToFinishUpdating(rg1, domainGroup);

    // Launch a smart client server
    startSmartClientServer();

    // open a dumb client (through the smart client)
    TTransport trans = new TFramedTransport(new TSocket("localhost", 50004));
    trans.open();
    SmartClient.Client dumbClient = new SmartClient.Client(new TCompactProtocol(trans));

    // make a few requests
    assertEquals(HankResponse.value(bb(1, 1)), dumbClient.get("domain0", bb(1)));
    assertEquals(HankResponse.value(bb(2, 2)), dumbClient.get("domain0", bb(2)));
    assertEquals(HankResponse.value(bb(3, 3)), dumbClient.get("domain0", bb(3)));
    assertEquals(HankResponse.value(bb(4, 4)), dumbClient.get("domain0", bb(4)));

    assertEquals(HankResponse.not_found(true), dumbClient.get("domain0", bb(99)));

    assertEquals(HankResponse.value(bb(1, 1)), dumbClient.get("domain1", bb(4)));
    assertEquals(HankResponse.value(bb(2, 2)), dumbClient.get("domain1", bb(3)));
    assertEquals(HankResponse.value(bb(3, 3)), dumbClient.get("domain1", bb(2)));
    assertEquals(HankResponse.value(bb(4, 4)), dumbClient.get("domain1", bb(1)));

    assertEquals(HankResponse.not_found(true), dumbClient.get("domain1", bb(99)));

    assertEquals(HankResponse.xception(HankException.no_such_domain(true)), dumbClient.get("domain2", bb(1)));

    // make a few bulk requests
    List<ByteBuffer> bulkRequest1 = new ArrayList<ByteBuffer>();
    bulkRequest1.add(bb(1));
    bulkRequest1.add(bb(2));
    bulkRequest1.add(bb(3));
    bulkRequest1.add(bb(4));
    List<HankResponse> bulkResponse1 = new ArrayList<HankResponse>();
    bulkResponse1.add(HankResponse.value(bb(1, 1)));
    bulkResponse1.add(HankResponse.value(bb(2, 2)));
    bulkResponse1.add(HankResponse.value(bb(3, 3)));
    bulkResponse1.add(HankResponse.value(bb(4, 4)));
    List<ByteBuffer> bulkRequest2 = new ArrayList<ByteBuffer>();
    bulkRequest2.add(bb(1));
    bulkRequest2.add(bb(99));
    List<HankResponse> bulkResponse2 = new ArrayList<HankResponse>();
    bulkResponse2.add(HankResponse.value(bb(1, 1)));
    bulkResponse2.add(HankResponse.not_found(true));

    assertEquals(HankBulkResponse.responses(bulkResponse1), dumbClient.getBulk("domain0", bulkRequest1));
    assertEquals(HankBulkResponse.responses(bulkResponse2), dumbClient.getBulk("domain0", bulkRequest2));
    assertEquals(HankBulkResponse.xception(HankException.no_such_domain(true)), dumbClient.getBulk("domain2", bulkRequest1));

    // push a new version of one of the domains
    Map<ByteBuffer, ByteBuffer> domain1Delta = new HashMap<ByteBuffer, ByteBuffer>();
    domain1Delta.put(bb(4), bb(41, 41));
    domain1Delta.put(bb(7), bb(42, 42));

    writeOut(coordinator.getDomain("domain1"), domain1Delta, 1, DOMAIN_1_DATAFILES);

    versionMap = new HashMap<Domain, Integer>();
    versionMap.put(coordinator.getDomain("domain0"), 0);
    versionMap.put(coordinator.getDomain("domain1"), 1);
    LOG.info("----- stamping new dg1 version -----");
    domainGroup.setDomainVersions(versionMap);

    // wait until domain group change propagates
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return domainGroup.getDomainVersion(coordinator.getDomain("domain1")).getVersionNumber() == 1;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    // wait until the rings have been updated to the new version
    waitForRingGroupToFinishUpdating(coordinator.getRingGroup("rg1"), domainGroup);

    // keep making requests
    assertEquals(HankResponse.value(bb(1, 1)), dumbClient.get("domain0", bb(1)));
    assertEquals(HankResponse.value(bb(2, 2)), dumbClient.get("domain0", bb(2)));
    assertEquals(HankResponse.value(bb(3, 3)), dumbClient.get("domain0", bb(3)));
    assertEquals(HankResponse.value(bb(4, 4)), dumbClient.get("domain0", bb(4)));

    assertEquals(HankResponse.not_found(true), dumbClient.get("domain0", bb(99)));

    assertEquals(HankResponse.value(bb(41, 41)), dumbClient.get("domain1", bb(4)));
    assertEquals(HankResponse.value(bb(2, 2)), dumbClient.get("domain1", bb(3)));
    assertEquals(HankResponse.value(bb(3, 3)), dumbClient.get("domain1", bb(2)));
    assertEquals(HankResponse.value(bb(4, 4)), dumbClient.get("domain1", bb(1)));
    assertEquals(HankResponse.value(bb(8, 4)), dumbClient.get("domain1", bb(5)));
    assertEquals(HankResponse.value(bb(42, 42)), dumbClient.get("domain1", bb(7)));

    assertEquals(HankResponse.xception(HankException.no_such_domain(true)), dumbClient.get("domain2", bb(1)));

    // take down hosts of one ring "unexpectedly"
    stopDaemons(new PartitionServerAddress("localhost", 50000));
    stopDaemons(new PartitionServerAddress("localhost", 50001));

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return HostState.OFFLINE.equals(r1h1.getState())
              && HostState.OFFLINE.equals(r1h2.getState());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    Thread.sleep(1000);

    // keep making requests
    assertEquals(HankResponse.value(bb(1, 1)), dumbClient.get("domain0", bb(1)));
    assertEquals(HankResponse.value(bb(2, 2)), dumbClient.get("domain0", bb(2)));
    assertEquals(HankResponse.value(bb(3, 3)), dumbClient.get("domain0", bb(3)));
    assertEquals(HankResponse.value(bb(4, 4)), dumbClient.get("domain0", bb(4)));

    assertEquals(HankResponse.not_found(true), dumbClient.get("domain0", bb(99)));

    assertEquals(HankResponse.value(bb(41, 41)), dumbClient.get("domain1", bb(4)));
    assertEquals(HankResponse.value(bb(2, 2)), dumbClient.get("domain1", bb(3)));
    assertEquals(HankResponse.value(bb(3, 3)), dumbClient.get("domain1", bb(2)));
    assertEquals(HankResponse.value(bb(4, 4)), dumbClient.get("domain1", bb(1)));
    assertEquals(HankResponse.value(bb(8, 4)), dumbClient.get("domain1", bb(5)));

    assertEquals(HankResponse.xception(HankException.no_such_domain(true)), dumbClient.get("domain2", bb(1)));

    // take down other ring "unexpectedly"
    stopDaemons(new PartitionServerAddress("localhost", 50002));
    stopDaemons(new PartitionServerAddress("localhost", 50003));

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return HostState.OFFLINE.equals(r2h1.getState())
              && HostState.OFFLINE.equals(r2h2.getState());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    // keep making requests
    assertEquals(HankResponse.xception(HankException.failed_retries(1)), dumbClient.get("domain0", bb(1)));
    assertEquals(HankResponse.xception(HankException.failed_retries(1)), dumbClient.get("domain0", bb(2)));
    assertEquals(HankResponse.xception(HankException.failed_retries(1)), dumbClient.get("domain0", bb(3)));
    assertEquals(HankResponse.xception(HankException.failed_retries(1)), dumbClient.get("domain0", bb(4)));

    assertEquals(HankResponse.xception(HankException.failed_retries(1)), dumbClient.get("domain0", bb(99)));

    assertEquals(HankResponse.xception(HankException.failed_retries(1)), dumbClient.get("domain1", bb(4)));
    assertEquals(HankResponse.xception(HankException.failed_retries(1)), dumbClient.get("domain1", bb(3)));
    assertEquals(HankResponse.xception(HankException.failed_retries(1)), dumbClient.get("domain1", bb(2)));
    assertEquals(HankResponse.xception(HankException.failed_retries(1)), dumbClient.get("domain1", bb(1)));
    assertEquals(HankResponse.xception(HankException.failed_retries(1)), dumbClient.get("domain1", bb(5)));

    // restart one ring
    startDaemons(new PartitionServerAddress("localhost", 50000));
    startDaemons(new PartitionServerAddress("localhost", 50001));

    // tell them to start serving
    Rings.commandAll(rg1r1, HostCommand.SERVE_DATA);

    // Wait until the ring is online
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          LOG.debug("Waiting for ring r1 to come back online");
          return r1h1.getState().equals(HostState.SERVING)
              && r1h2.getState().equals(HostState.SERVING);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    // keep making requests
    assertEquals(HankResponse.value(bb(1, 1)), dumbClient.get("domain0", bb(1)));
    assertEquals(HankResponse.value(bb(2, 2)), dumbClient.get("domain0", bb(2)));
    assertEquals(HankResponse.value(bb(3, 3)), dumbClient.get("domain0", bb(3)));
    assertEquals(HankResponse.value(bb(4, 4)), dumbClient.get("domain0", bb(4)));

    assertEquals(HankResponse.not_found(true), dumbClient.get("domain0", bb(99)));

    assertEquals(HankResponse.value(bb(41, 41)), dumbClient.get("domain1", bb(4)));
    assertEquals(HankResponse.value(bb(2, 2)), dumbClient.get("domain1", bb(3)));
    assertEquals(HankResponse.value(bb(3, 3)), dumbClient.get("domain1", bb(2)));
    assertEquals(HankResponse.value(bb(4, 4)), dumbClient.get("domain1", bb(1)));
    assertEquals(HankResponse.value(bb(8, 4)), dumbClient.get("domain1", bb(5)));

    assertEquals(HankResponse.xception(HankException.no_such_domain(true)), dumbClient.get("domain2", bb(1)));

    // shut it all down
    stopRingGroupConductor();
    stopSmartClient();

    stopDaemons(new PartitionServerAddress("localhost", 50000));
    stopDaemons(new PartitionServerAddress("localhost", 50001));
  }

  private void waitForRingGroupToFinishUpdating(final RingGroup rg, final DomainGroup domainGroup)
      throws IOException, InterruptedException {
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        LOG.info("Ring group is not yet at the correct version. Continuing to wait.");
        try {
          return RingGroups.isUpToDate(rg, domainGroup)
              && RingGroups.getHostsInState(rg, HostState.SERVING).size() == RingGroups.getNumHosts(rg);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }, 60 * 1000);
  }

  private void startSmartClientServer() throws Exception {
    LOG.debug("starting smart client server...");
    smartClientRunnable = new SmartClientRunnable();
    smartClientRunnable.run();
  }

  private void stopSmartClient() throws Exception {
    smartClientRunnable.pleaseStop();
  }

  private void startRingGroupConductor() throws Exception {
    LOG.debug("starting Ring Group Conductor");
    ringGroupConductorRunnable = new RingGroupConductorRunnable(ConfigFixtures.createRGCConfigurator(
        localTmpDir,
        getZkClientPort(),
        "rg1",
        RingGroupConductorMode.ACTIVE,
        null,
        null,
        domainsRoot,
        domainGroupsRoot,
        ringGroupsRoot,
        Lists.newArrayList()
    ));
    ringGroupConductorThread = new Thread(ringGroupConductorRunnable, "Ring Group Conductor thread");
    ringGroupConductorThread.start();
  }

  private void stopRingGroupConductor() throws Exception {
    LOG.debug("stopping Ring Group Conductor");
    ringGroupConductorRunnable.pleaseStop();
    ringGroupConductorThread.join();
  }

  private void startDaemons(PartitionServerAddress a) throws Exception {
    LOG.debug("Starting partition servers for " + a);

    PartitionServerRunnable pr = new PartitionServerRunnable(
        createConfigurator(a)
    );
    partitionServerRunnables.put(a, pr);
    Thread pt = new Thread(pr, "partition server thread for " + a);
    partitionServerThreads.put(a, pt);
    pt.start();
  }

  private PartitionServerConfigurator createConfigurator(PartitionServerAddress a) throws IOException, InvalidConfigurationException {

    String hostDotPort = a.getHostName() + "." + a.getPortNumber();
    String dataDir = localTmpDir + "/" + hostDotPort;
    String configPath = localTmpDir + "/" + hostDotPort + ".partition_server.yml";

    String configYaml = ConfigFixtures.partitionServerConfig(dataDir, "rg1", getZkClientPort(), a, domainsRoot, domainGroupsRoot, ringGroupsRoot);

    PrintWriter pw = new PrintWriter(new FileWriter(configPath));
    pw.write(configYaml);
    pw.close();

    return new YamlPartitionServerConfigurator(configPath);
  }

  private void stopDaemons(PartitionServerAddress a) throws Exception {
    LOG.debug("Stopping partition servers for " + a);
    partitionServerRunnables.get(a).pleaseStop();
    partitionServerThreads.get(a).join();
  }

  private void writeOut(final Domain domain, Map<ByteBuffer, ByteBuffer> dataItems, int versionNumber, String domainRoot) throws IOException, InterruptedException {
    // Create new version
    domain.openNewVersion(new IncrementalDomainVersionProperties(versionNumber == 0 ? null : versionNumber - 1)).close();
    Thread.sleep(100);
    LOG.debug("Writing out new version " + versionNumber + " of domain " + domain.getName() + " to root " + domainRoot);
    assertEquals(versionNumber, Domains.getLatestVersionNotOpenNotDefunct(domain).getVersionNumber());
    // partition keys and values
    Map<Integer, SortedMap<ByteBuffer, ByteBuffer>> sortedAndPartitioned = new HashMap<Integer, SortedMap<ByteBuffer, ByteBuffer>>();
    Partitioner p = domain.getPartitioner();
    for (Map.Entry<ByteBuffer, ByteBuffer> pair : dataItems.entrySet()) {
      int partNum = p.partition(pair.getKey(), domain.getNumParts());
      SortedMap<ByteBuffer, ByteBuffer> part = sortedAndPartitioned.get(partNum);
      if (part == null) {
        part = new TreeMap<ByteBuffer, ByteBuffer>(new Comparator<ByteBuffer>() {
          public int compare(ByteBuffer arg0, ByteBuffer arg1) {
            final StorageEngine storageEngine = domain.getStorageEngine();
            final ByteBuffer keyL = BytesUtils.byteBufferDeepCopy(storageEngine.getComparableKey(arg0));
            final ByteBuffer keyR = storageEngine.getComparableKey(arg1);
            return BytesUtils.compareBytesUnsigned(keyL.array(), keyL.position(), keyR.array(), keyR.position(), keyL.remaining());
          }
        });
        sortedAndPartitioned.put(partNum, part);
      }
      LOG.trace(String.format("putting %s -> %s into partition %d", BytesUtils.bytesToHexString(pair.getKey()), BytesUtils.bytesToHexString(pair.getValue()), partNum));
      part.put(pair.getKey(), pair.getValue());
    }
    StorageEngine engine = domain.getStorageEngine();
    new File(domainRoot).mkdirs();
    for (Map.Entry<Integer, SortedMap<ByteBuffer, ByteBuffer>> part : sortedAndPartitioned.entrySet()) {
      LOG.debug("Writing out part " + part.getKey() + " for domain " + domain.getName() + " to root " + domainRoot);
      Writer writer = engine.getWriter(domain.getVersion(versionNumber),
          new LocalPartitionRemoteFileOps(domainRoot, part.getKey()), part.getKey());
      final SortedMap<ByteBuffer, ByteBuffer> partPairs = part.getValue();
      for (Map.Entry<ByteBuffer, ByteBuffer> pair : partPairs.entrySet()) {
        LOG.trace(String.format("writing %s -> %s", BytesUtils.bytesToHexString(pair.getKey()), BytesUtils.bytesToHexString(pair.getValue())));
        writer.write(pair.getKey(), pair.getValue());
      }
      writer.close();
    }
  }

  private static ByteBuffer bb(int... bs) {
    byte[] bytes = new byte[bs.length];
    for (int i = 0; i < bs.length; i++) {
      bytes[i] = (byte)bs[i];
    }
    return ByteBuffer.wrap(bytes);
  }

}
