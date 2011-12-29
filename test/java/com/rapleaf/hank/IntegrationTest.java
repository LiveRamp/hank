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
package com.rapleaf.hank;

import com.rapleaf.hank.compress.JavaGzipCompressionCodec;
import com.rapleaf.hank.config.CoordinatorConfigurator;
import com.rapleaf.hank.config.PartitionServerConfigurator;
import com.rapleaf.hank.config.RingGroupConductorConfigurator;
import com.rapleaf.hank.config.SmartClientDaemonConfigurator;
import com.rapleaf.hank.config.yaml.YamlClientConfigurator;
import com.rapleaf.hank.config.yaml.YamlPartitionServerConfigurator;
import com.rapleaf.hank.config.yaml.YamlRingGroupConductorConfigurator;
import com.rapleaf.hank.config.yaml.YamlSmartClientDaemonConfigurator;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;
import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.SmartClient;
import com.rapleaf.hank.hasher.Murmur64Hasher;
import com.rapleaf.hank.partition_server.PartitionServer;
import com.rapleaf.hank.partitioner.Murmur64Partitioner;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.ring_group_conductor.RingGroupConductor;
import com.rapleaf.hank.storage.*;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.storage.curly.Curly;
import com.rapleaf.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.rapleaf.hank.util.Bytes;
import com.rapleaf.hank.zookeeper.ZkPath;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class IntegrationTest extends ZkTestCase {

  private final class SmartClientRunnable implements Runnable {

    private final String configPath;
    private com.rapleaf.hank.client.SmartClientDaemon server;
    private final SmartClientDaemonConfigurator configurator;

    public SmartClientRunnable() throws Exception {
      this.configPath = localTmpDir + "/smart_client_config.yml";
      PrintWriter pw = new PrintWriter(new FileWriter(configPath));
      pw.println(YamlSmartClientDaemonConfigurator.SMART_CLIENT_SECTION_KEY + ":");
      pw.println("  " + YamlSmartClientDaemonConfigurator.SERVICE_PORT_KEY + ": 50004");
      pw.println("  " + YamlSmartClientDaemonConfigurator.NUM_WORKER_THREADS + ": 1");
      pw.println("  " + YamlSmartClientDaemonConfigurator.RING_GROUP_NAME_KEY + ": rg1");
      coordinatorConfig(pw);
      pw.close();
      configurator = new YamlSmartClientDaemonConfigurator(configPath);
    }

    public void run() {
      server = new com.rapleaf.hank.client.SmartClientDaemon(configurator);
      server.startServer();
    }

    public void pleaseStop() {
      server.downServer();
    }
  }

  private final class RingGroupConductorRunnable implements Runnable {
    private RingGroupConductorConfigurator configurator;
    private RingGroupConductor daemon;

    public RingGroupConductorRunnable() throws Exception {
      String configPath = localTmpDir + "/ring_group_conductor_config.yml";
      PrintWriter pw = new PrintWriter(new FileWriter(configPath));
      pw.println(YamlRingGroupConductorConfigurator.RING_GROUP_CONDUCTOR_SECTION_KEY + ":");
      pw.println("  " + YamlRingGroupConductorConfigurator.SLEEP_INTERVAL_KEY + ": 1000");
      pw.println("  " + YamlRingGroupConductorConfigurator.RING_GROUP_NAME_KEY + ": rg1");
      pw.println("  " + YamlRingGroupConductorConfigurator.INITIAL_MODE_KEY + ": ACTIVE");
      coordinatorConfig(pw);
      pw.close();
      configurator = new YamlRingGroupConductorConfigurator(configPath);
    }

    public void run() {
      try {
        daemon = new RingGroupConductor(configurator);
        daemon.run();
      } catch (Exception e) {
        LOG.fatal("crap, some exception", e);
      }
    }

    public void pleaseStop() {
      daemon.stop();
    }
  }

  private final class PartitionServerRunnable implements Runnable {
    private final String configPath;
    @SuppressWarnings("unused")
    private Throwable throwable;
    private PartitionServer server;
    private final PartitionServerConfigurator configurator;

    public PartitionServerRunnable(PartitionServerAddress addy) throws Exception {
      String hostDotPort = addy.getHostName()
          + "." + addy.getPortNumber();
      this.configPath = localTmpDir + "/" + hostDotPort + ".partition_server.yml";

      PrintWriter pw = new PrintWriter(new FileWriter(configPath));
      pw.println(YamlPartitionServerConfigurator.PARTITION_SERVER_SECTION_KEY + ":");
      pw.println("  " + YamlPartitionServerConfigurator.SERVICE_PORT_KEY + ": " + addy.getPortNumber());
      pw.println("  " + YamlPartitionServerConfigurator.RING_GROUP_NAME_KEY + ": rg1");
      pw.println("  " + YamlPartitionServerConfigurator.LOCAL_DATA_DIRS_KEY + ":");
      pw.println("    - " + localTmpDir + "/" + hostDotPort);
      pw.println("  " + YamlPartitionServerConfigurator.PARTITION_SERVER_DAEMON_SECTION_KEY + ":");
      pw.println("    " + YamlPartitionServerConfigurator.NUM_CONCURRENT_QUERIES_KEY + ": 1");
      pw.println("    " + YamlPartitionServerConfigurator.NUM_CONCURRENT_GET_BULK_TASKS + ": 1");
      pw.println("    " + YamlPartitionServerConfigurator.GET_BULK_TASK_SIZE + ": 2");
      pw.println("    " + YamlPartitionServerConfigurator.GET_TIMER_AGGREGATOR_WINDOW_KEY + ": 1000");
      pw.println("  " + YamlPartitionServerConfigurator.UPDATE_DAEMON_SECTION_KEY + ":");
      pw.println("    " + YamlPartitionServerConfigurator.NUM_CONCURRENT_UPDATES_KEY + ": 1");
      coordinatorConfig(pw);
      pw.close();
      configurator = new YamlPartitionServerConfigurator(configPath);
    }

    public void run() {
      try {
        server = new PartitionServer(configurator, "localhost");
        server.run();
      } catch (Throwable t) {
        LOG.fatal("crap, some exception...", t);
        throwable = t;
      }
    }

    public void pleaseStop() throws Exception {
      server.stopSynchronized();
    }
  }

  private static final Logger LOG = Logger.getLogger(IntegrationTest.class);
  private final String DOMAIN_0_DATAFILES = localTmpDir + "/domain0_datafiles";
  private final String DOMAIN_1_DATAFILES = localTmpDir + "/domain1_datafiles";

  private final String domainsRoot = ZkPath.append(getRoot(), "domains");
  private final String domainGroupsRoot = ZkPath.append(getRoot(), "domain_groups");
  private final String ringGroupsRoot = ZkPath.append(getRoot(), "ring_groups");
  private final String clientConfigYml = localTmpDir + "/config.yml";
  private final String domain0OptsYml = localTmpDir + "/domain0_opts.yml";
  private final String domain1OptsYml = localTmpDir + "/domain1_opts.yml";
  private final Map<PartitionServerAddress, Thread> partitionServerThreads = new HashMap<PartitionServerAddress, Thread>();
  private final Map<PartitionServerAddress, PartitionServerRunnable> partitionServerRunnables = new HashMap<PartitionServerAddress, PartitionServerRunnable>();

  private Thread ringGroupConductorThread;
  private RingGroupConductorRunnable ringGroupConductorRunnable;
  private SmartClientRunnable smartClientRunnable;

  public void testItAll() throws Throwable {
    Logger.getLogger("com.rapleaf.hank.coordinator.zk").setLevel(Level.INFO);
    // Logger.getLogger("com.rapleaf.hank.partition_server").setLevel(Level.INFO);
    Logger.getLogger("com.rapleaf.hank.storage").setLevel(Level.TRACE);
    create(domainsRoot);
    create(domainGroupsRoot);
    create(ringGroupsRoot);

    PrintWriter pw = new PrintWriter(new FileWriter(clientConfigYml));
    pw.println("coordinator:");
    pw.println("  factory: com.rapleaf.hank.coordinator.zk.ZooKeeperCoordinator$Factory");
    pw.println("  options:");
    pw.println("    connect_string: localhost:" + getZkClientPort());
    pw.println("    session_timeout: 1000000");
    pw.println("    domains_root: " + domainsRoot);
    pw.println("    domain_groups_root: " + domainGroupsRoot);
    pw.println("    ring_groups_root: " + ringGroupsRoot);
    pw.close();

    CoordinatorConfigurator config = new YamlClientConfigurator(clientConfigYml);

    Coordinator coordinator = config.createCoordinator();

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
    coordinator.addDomain("domain0", 2, Curly.Factory.class.getName(), sw.toString(), Murmur64Partitioner.class.getName());

    sw = new StringWriter();
    pw = new PrintWriter(sw);
    pw.println("key_hash_size: 10");
    pw.println("hasher: " + Murmur64Hasher.class.getName());
    pw.println("max_allowed_part_size: " + 1024 * 1024);
    pw.println("hash_index_bits: 1");
    pw.println("record_file_read_buffer_bytes: 10240");
    pw.println("remote_domain_root: " + DOMAIN_1_DATAFILES);
    pw.println("file_ops_factory: " + LocalPartitionRemoteFileOps.Factory.class.getName());
    pw.println("compression_codec: " + JavaGzipCompressionCodec.class.getName());
    pw.println("num_remote_leaf_versions_to_keep: 0");
    pw.close();
    coordinator.addDomain("domain1", 2, Curly.Factory.class.getName(), sw.toString(), Murmur64Partitioner.class.getName());

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

    writeOut(coordinator.getDomain("domain0"), domain0DataItems, 0, true, DOMAIN_0_DATAFILES);

    Map<ByteBuffer, ByteBuffer> domain1DataItems = new HashMap<ByteBuffer, ByteBuffer>();
    domain1DataItems.put(bb(4), bb(1, 1));
    domain1DataItems.put(bb(3), bb(2, 2));
    domain1DataItems.put(bb(2), bb(3, 3));
    domain1DataItems.put(bb(1), bb(4, 4));
    domain1DataItems.put(bb(8), bb(5, 1));
    domain1DataItems.put(bb(7), bb(6, 2));
    domain1DataItems.put(bb(6), bb(7, 3));
    domain1DataItems.put(bb(5), bb(8, 4));

    writeOut(coordinator.getDomain("domain1"), domain1DataItems, 0, true, DOMAIN_1_DATAFILES);

    // configure domain group
    coordinator.addDomainGroup("dg1");

    LOG.debug("-------- domain is created --------");

    // simulate publisher pushing out a new version
    DomainGroup domainGroup = null;
    coordinator = config.createCoordinator();
    for (int i = 0; i < 15; i++) {
      domainGroup = coordinator.getDomainGroup("dg1");
      if (domainGroup != null) {
        break;
      }
      Thread.sleep(1000);
    }
    assertNotNull("dg1 wasn't found, even after waiting 15 seconds!", domainGroup);

    Map<Domain, Integer> versionMap = new HashMap<Domain, Integer>();
    versionMap.put(coordinator.getDomain("domain0"), 0);
    versionMap.put(coordinator.getDomain("domain1"), 0);
    domainGroup.createNewVersion(versionMap);

    // configure ring group
    final RingGroup rg1 = coordinator.addRingGroup("rg1", "dg1");

    // add ring 1
    final Ring rg1r1 = rg1.addRing(1);
    Host r1h1 = rg1r1.addHost(PartitionServerAddress.parse("localhost:50000"));
    Host r1h2 = rg1r1.addHost(PartitionServerAddress.parse("localhost:50001"));

    // add ring 2
    final Ring rg1r2 = rg1.addRing(2);
    Host r2h1 = rg1r2.addHost(PartitionServerAddress.parse("localhost:50002"));
    Host r2h2 = rg1r2.addHost(PartitionServerAddress.parse("localhost:50003"));

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
    r1h1d0.addPartition(0, 0);
    r1h2d0.addPartition(1, 0);
    r2h1d0.addPartition(0, 0);
    r2h2d0.addPartition(1, 0);
    // Domain1
    r1h1d1.addPartition(0, 0);
    r1h2d1.addPartition(1, 0);
    r2h1d1.addPartition(0, 0);
    r2h2d1.addPartition(1, 0);

    // launch 2x 2-node rings
    startDaemons(new PartitionServerAddress("localhost", 50000));
    startDaemons(new PartitionServerAddress("localhost", 50001));
    startDaemons(new PartitionServerAddress("localhost", 50002));
    startDaemons(new PartitionServerAddress("localhost", 50003));

    // launch the Ring Group Conductor
    startRingGroupConductor();

    // Wait for update to finish
    waitForRingGroupToFinishUpdating(rg1, DomainGroups.getLatestVersion(domainGroup).getVersionNumber());

    // Launch a smart client server
    startSmartClientServer();

    // open a dumb client (through the smart client)
    TTransport trans = new TFramedTransport(new TSocket("localhost", 50004));
    trans.open();
    TProtocol proto = new TCompactProtocol(trans);
    SmartClient.Client dumbClient = new SmartClient.Client(proto);

    boolean found = false;
    for (int i = 0; i < 15; i++) {
      try {
        HankResponse r = dumbClient.get("domain0", bb(1));
        LOG.trace(r);
        if (r.isSet(HankResponse._Fields.VALUE)) {
          found = true;
          break;
        }
      } catch (TException e) {
        LOG.error(e);
      }
      Thread.sleep(1000);
    }

    if (!found) {
      fail("No ring came online in the time we waited!");
    }

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
    domain1Delta.put(bb(4), bb(6, 6));
    domain1Delta.put(bb(5), bb(5, 5));

    writeOut(coordinator.getDomain("domain1"), domain1Delta, 1, false, DOMAIN_1_DATAFILES);

    versionMap = new HashMap<Domain, Integer>();
    versionMap.put(coordinator.getDomain("domain0"), 0);
    versionMap.put(coordinator.getDomain("domain1"), 1);
    LOG.info("----- stamping new dg1 version -----");
    final DomainGroupVersion newVersion = domainGroup.createNewVersion(versionMap);

    // wait until the rings have been updated to the new version
    waitForRingGroupToFinishUpdating(coordinator.getRingGroup("rg1"), newVersion.getVersionNumber());

    /*
    while (!HankResponse.value(bb(6, 6)).equals(dumbClient.get("domain1", bb(4)))) {
      LOG.info("#### Waiting for ring to be updated by querying for a specific value");
    }
    LOG.info("#### Exited, specific value was correct #### !!!!!!!!!!!!!!!!!!!!!!!!!!!");
    */

    // keep making requests
    assertEquals(HankResponse.value(bb(1, 1)), dumbClient.get("domain0", bb(1)));
    assertEquals(HankResponse.value(bb(2, 2)), dumbClient.get("domain0", bb(2)));
    assertEquals(HankResponse.value(bb(3, 3)), dumbClient.get("domain0", bb(3)));
    assertEquals(HankResponse.value(bb(4, 4)), dumbClient.get("domain0", bb(4)));

    assertEquals(HankResponse.not_found(true), dumbClient.get("domain0", bb(99)));

    assertEquals(HankResponse.value(bb(6, 6)), dumbClient.get("domain1", bb(4)));
    assertEquals(HankResponse.value(bb(2, 2)), dumbClient.get("domain1", bb(3)));
    assertEquals(HankResponse.value(bb(3, 3)), dumbClient.get("domain1", bb(2)));
    assertEquals(HankResponse.value(bb(4, 4)), dumbClient.get("domain1", bb(1)));
    assertEquals(HankResponse.value(bb(5, 5)), dumbClient.get("domain1", bb(5)));

    assertEquals(HankResponse.xception(HankException.no_such_domain(true)), dumbClient.get("domain2", bb(1)));

    // take down hosts of one ring "unexpectedly"
    stopDaemons(new PartitionServerAddress("localhost", 50000));
    stopDaemons(new PartitionServerAddress("localhost", 50001));
    Thread.sleep(1000);

    // keep making requests
    assertEquals(HankResponse.value(bb(1, 1)), dumbClient.get("domain0", bb(1)));
    assertEquals(HankResponse.value(bb(2, 2)), dumbClient.get("domain0", bb(2)));
    assertEquals(HankResponse.value(bb(3, 3)), dumbClient.get("domain0", bb(3)));
    assertEquals(HankResponse.value(bb(4, 4)), dumbClient.get("domain0", bb(4)));

    assertEquals(HankResponse.not_found(true), dumbClient.get("domain0", bb(99)));

    assertEquals(HankResponse.value(bb(6, 6)), dumbClient.get("domain1", bb(4)));
    assertEquals(HankResponse.value(bb(2, 2)), dumbClient.get("domain1", bb(3)));
    assertEquals(HankResponse.value(bb(3, 3)), dumbClient.get("domain1", bb(2)));
    assertEquals(HankResponse.value(bb(4, 4)), dumbClient.get("domain1", bb(1)));
    assertEquals(HankResponse.value(bb(5, 5)), dumbClient.get("domain1", bb(5)));

    assertEquals(HankResponse.xception(HankException.no_such_domain(true)), dumbClient.get("domain2", bb(1)));

    // take down other ring "unexpectedly"
    stopDaemons(new PartitionServerAddress("localhost", 50002));
    stopDaemons(new PartitionServerAddress("localhost", 50003));
    Thread.sleep(1000);

    // keep making requests
    assertEquals(HankResponse.xception(HankException.no_connection_available(true)), dumbClient.get("domain0", bb(1)));
    assertEquals(HankResponse.xception(HankException.no_connection_available(true)), dumbClient.get("domain0", bb(2)));
    assertEquals(HankResponse.xception(HankException.no_connection_available(true)), dumbClient.get("domain0", bb(3)));
    assertEquals(HankResponse.xception(HankException.no_connection_available(true)), dumbClient.get("domain0", bb(4)));

    assertEquals(HankResponse.xception(HankException.no_connection_available(true)), dumbClient.get("domain0", bb(99)));

    assertEquals(HankResponse.xception(HankException.no_connection_available(true)), dumbClient.get("domain1", bb(4)));
    assertEquals(HankResponse.xception(HankException.no_connection_available(true)), dumbClient.get("domain1", bb(3)));
    assertEquals(HankResponse.xception(HankException.no_connection_available(true)), dumbClient.get("domain1", bb(2)));
    assertEquals(HankResponse.xception(HankException.no_connection_available(true)), dumbClient.get("domain1", bb(1)));
    assertEquals(HankResponse.xception(HankException.no_connection_available(true)), dumbClient.get("domain1", bb(5)));

    // restart one ring
    startDaemons(new PartitionServerAddress("localhost", 50000));
    startDaemons(new PartitionServerAddress("localhost", 50001));

    // tell them to start serving
    Rings.commandAll(rg1r1, HostCommand.SERVE_DATA);

    // Wait until the ring is online
    for (int i = 0; i < 30; ++i) {
      if (r1h1.getState().equals(HostState.SERVING)
          && r1h2.getState().equals(HostState.SERVING)) {
        break;
      }
      LOG.debug("Waiting for ring r1 to come back online");
      Thread.sleep(1000);
    }

    // keep making requests
    assertEquals(HankResponse.value(bb(1, 1)), dumbClient.get("domain0", bb(1)));
    assertEquals(HankResponse.value(bb(2, 2)), dumbClient.get("domain0", bb(2)));
    assertEquals(HankResponse.value(bb(3, 3)), dumbClient.get("domain0", bb(3)));
    assertEquals(HankResponse.value(bb(4, 4)), dumbClient.get("domain0", bb(4)));

    assertEquals(HankResponse.not_found(true), dumbClient.get("domain0", bb(99)));

    assertEquals(HankResponse.value(bb(6, 6)), dumbClient.get("domain1", bb(4)));
    assertEquals(HankResponse.value(bb(2, 2)), dumbClient.get("domain1", bb(3)));
    assertEquals(HankResponse.value(bb(3, 3)), dumbClient.get("domain1", bb(2)));
    assertEquals(HankResponse.value(bb(4, 4)), dumbClient.get("domain1", bb(1)));
    assertEquals(HankResponse.value(bb(5, 5)), dumbClient.get("domain1", bb(5)));

    assertEquals(HankResponse.xception(HankException.no_such_domain(true)), dumbClient.get("domain2", bb(1)));

    // shut it all down
    stopRingGroupConductor();
    stopSmartClient();

    stopDaemons(new PartitionServerAddress("localhost", 50000));
    stopDaemons(new PartitionServerAddress("localhost", 50001));
  }

  private void waitForRingGroupToFinishUpdating(RingGroup rg, int versionNumber) throws IOException, InterruptedException {
    for (int i = 0; i < 30; i++) {
      if (RingGroups.isUpdating(rg)) {
        LOG.info("Ring group is still updating. Sleeping...");
      } else {
        if (rg.getCurrentVersionNumber() != null && rg.getCurrentVersionNumber() == versionNumber) {
          break;
        } else {
          LOG.info("Ring group is not yet at the correct version. Continuing to wait.");
        }
      }
      Thread.sleep(1000);
    }
    assertFalse("Ring group failed to finish updating after 30secs", RingGroups.isUpdating(rg));
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
    ringGroupConductorRunnable = new RingGroupConductorRunnable();
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
    PartitionServerRunnable pr = new PartitionServerRunnable(a);
    partitionServerRunnables.put(a, pr);
    Thread pt = new Thread(pr, "partition server thread for " + a);
    partitionServerThreads.put(a, pt);
    pt.start();
  }

  private void stopDaemons(PartitionServerAddress a) throws Exception {
    LOG.debug("Stopping partition servers for " + a);
    partitionServerRunnables.get(a).pleaseStop();
    partitionServerThreads.get(a).join();
  }

  private void writeOut(final Domain domain, Map<ByteBuffer, ByteBuffer> dataItems, int versionNumber, boolean isBase, String domainRoot) throws IOException {
    // Create new version
    domain.openNewVersion(null).close();
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
            final ByteBuffer keyL = Bytes.byteBufferDeepCopy(storageEngine.getComparableKey(arg0));
            final ByteBuffer keyR = storageEngine.getComparableKey(arg1);
            return Bytes.compareBytesUnsigned(keyL.array(), keyL.position(), keyR.array(), keyR.position(), keyL.remaining());
          }
        });
        sortedAndPartitioned.put(partNum, part);
      }
      LOG.trace(String.format("putting %s -> %s into partition %d", Bytes.bytesToHexString(pair.getKey()), Bytes.bytesToHexString(pair.getValue()), partNum));
      part.put(pair.getKey(), pair.getValue());
    }
    StorageEngine engine = domain.getStorageEngine();
    new File(domainRoot).mkdirs();
    for (Map.Entry<Integer, SortedMap<ByteBuffer, ByteBuffer>> part : sortedAndPartitioned.entrySet()) {
      LOG.debug("Writing out part " + part.getKey() + " for domain " + domain.getName() + " to root " + domainRoot);
      Writer writer = engine.getWriter(new MockDomainVersion(versionNumber, null,
          new IncrementalDomainVersionProperties(versionNumber == 0 ? null : versionNumber - 1)),
          new LocalDiskOutputStreamFactory(domainRoot), part.getKey());
      final SortedMap<ByteBuffer, ByteBuffer> partPairs = part.getValue();
      for (Map.Entry<ByteBuffer, ByteBuffer> pair : partPairs.entrySet()) {
        LOG.trace(String.format("writing %s -> %s", Bytes.bytesToHexString(pair.getKey()), Bytes.bytesToHexString(pair.getValue())));
        writer.write(pair.getKey(), pair.getValue());
      }
      writer.close();
    }
  }

  private static ByteBuffer bb(int... bs) {
    byte[] bytes = new byte[bs.length];
    for (int i = 0; i < bs.length; i++) {
      bytes[i] = (byte) bs[i];
    }
    return ByteBuffer.wrap(bytes);
  }

  private void coordinatorConfig(PrintWriter pw) {
    pw.println("coordinator:");
    pw.println("  factory: com.rapleaf.hank.coordinator.zk.ZooKeeperCoordinator$Factory");
    pw.println("  options:");
    pw.println("    connect_string: localhost:" + getZkClientPort());
    pw.println("    session_timeout: 1000000");
    pw.println("    domains_root: " + domainsRoot);
    pw.println("    domain_groups_root: " + domainGroupsRoot);
    pw.println("    ring_groups_root: " + ringGroupsRoot);
  }
}
