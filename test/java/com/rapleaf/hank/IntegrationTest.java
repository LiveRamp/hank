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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.rapleaf.hank.cli.AddDomain;
import com.rapleaf.hank.cli.AddDomainGroup;
import com.rapleaf.hank.cli.AddDomainToDomainGroup;
import com.rapleaf.hank.cli.AddRing;
import com.rapleaf.hank.cli.AddRingGroup;
import com.rapleaf.hank.compress.JavaGzipCompressionCodec;
import com.rapleaf.hank.config.Configurator;
import com.rapleaf.hank.config.DataDeployerConfigurator;
import com.rapleaf.hank.config.PartservConfigurator;
import com.rapleaf.hank.config.SmartClientDaemonConfigurator;
import com.rapleaf.hank.config.yaml.YamlClientConfigurator;
import com.rapleaf.hank.config.yaml.YamlDataDeployerConfigurator;
import com.rapleaf.hank.config.yaml.YamlPartservConfigurator;
import com.rapleaf.hank.config.yaml.YamlSmartClientDaemonConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfigVersion;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.data_deployer.DataDeployer;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.SmartClient;
import com.rapleaf.hank.generated.HankResponse._Fields;
import com.rapleaf.hank.hasher.Murmur64Hasher;
import com.rapleaf.hank.partitioner.Murmur64Partitioner;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.LocalDiskOutputStreamFactory;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.storage.cueball.LocalFileOps;
import com.rapleaf.hank.storage.curly.Curly;
import com.rapleaf.hank.util.Bytes;

public class IntegrationTest extends ZkTestCase {
  private final class SmartClientRunnable implements Runnable {
    private final String configPath;
    private com.rapleaf.hank.client.SmartClientDaemon server;
    private final SmartClientDaemonConfigurator configurator;

    public SmartClientRunnable() throws Exception {
      this.configPath = localTmpDir + "/smart_client_config.yml";
      PrintWriter pw = new PrintWriter(new FileWriter(configPath));
      pw.println("smart_client:");
      pw.println("  service_port: 50004");
      pw.println("  num_worker_threads: 1");
      pw.println("  ring_group_name: rg1");
      coordinatorConfig(pw);
      pw.close();
      configurator = new YamlSmartClientDaemonConfigurator(configPath);
    }

    @Override
    public void run() {
      server = new com.rapleaf.hank.client.SmartClientDaemon(configurator);
      server.startServer();
    }

    public void pleaseStop() {
      server.downServer();
    }
  }

  private final class DataDeployerRunnable implements Runnable {
    private DataDeployerConfigurator configurator;
    private DataDeployer daemon;

    public DataDeployerRunnable() throws Exception {
      String configPath = localTmpDir + "/data_deployer_config.yml";
      PrintWriter pw = new PrintWriter(new FileWriter(configPath));
      pw.println("data_deployer:");
      pw.println("  sleep_interval: 1000");
      pw.println("  ring_group_name: rg1");
      coordinatorConfig(pw);
      pw.close();
      configurator = new YamlDataDeployerConfigurator(configPath);
    }

    @Override
    public void run() {
      try {
        daemon = new DataDeployer(configurator);
        daemon.run();
      } catch (Exception e) {
        LOG.fatal("crap, some exception", e);
      }
    }

    public void pleaseStop() {
      daemon.stop();
    }
  }

  private final class PartDaemonRunnable implements Runnable {
    private final String configPath;
    @SuppressWarnings("unused")
    private Throwable throwable;
    private com.rapleaf.hank.part_daemon.Server server;
    private final PartservConfigurator configurator;

    public PartDaemonRunnable(PartDaemonAddress addy) throws Exception {
      String hostDotPort = addy.getHostName()
                + "." + addy.getPortNumber();
      this.configPath = localTmpDir + "/" + hostDotPort + ".part_daemon.yml";

      PrintWriter pw = new PrintWriter(new FileWriter(configPath));
      pw.println("---");
      pw.println("partserv:");
      pw.println("  service_port: " + addy.getPortNumber());
      pw.println("  ring_group_name: rg1");
      pw.println("  local_data_dirs:");
      pw.println("    - " + localTmpDir + "/" + hostDotPort);
      pw.println("  part_daemon:");
      pw.println("    num_worker_threads: 1");
      pw.println("  update_daemon:");
      pw.println("    num_concurrent_updates: 1");
      coordinatorConfig(pw);
      pw.close();
      configurator = new YamlPartservConfigurator(configPath);
    }

    @Override
    public void run() {
      try {
        server = new com.rapleaf.hank.part_daemon.Server(configurator, "localhost");
        server.run();
      } catch (Throwable t) {
        LOG.fatal("crap, some exception...", t);
        throwable = t;
      }
    }

    public void pleaseStop() throws Exception {
      server.stop();
    }

  }

  private static final Logger LOG = Logger.getLogger(IntegrationTest.class);
  private final String DOMAIN_0_DATAFILES = localTmpDir + "/domain0_datafiles";
  private final String DOMAIN_1_DATAFILES = localTmpDir + "/domain1_datafiles";

  private final String domainsRoot = getRoot() + "/domains";
  private final String domainGroupsRoot = getRoot() + "/domain_groups";
  private final String ringGroupsRoot = getRoot() + "/ring_groups";
  private final String clientConfigYml = localTmpDir + "/config.yml";
  private final String domain0OptsYml = localTmpDir + "/domain0_opts.yml";
  private final String domain1OptsYml = localTmpDir + "/domain1_opts.yml";
  private final Map<PartDaemonAddress, Thread> partDaemonThreads = new HashMap<PartDaemonAddress, Thread>();
  private final Map<PartDaemonAddress, PartDaemonRunnable> partDaemonRunnables = new HashMap<PartDaemonAddress, PartDaemonRunnable>();

  private Thread dataDeployerThread;
  private DataDeployerRunnable dataDeployerRunnable;
  private SmartClientRunnable smartClientRunnable;

  public void testItAll() throws Throwable {
    Logger.getLogger("com.rapleaf.hank.coordinator.zk").setLevel(Level.INFO);
    Logger.getLogger("com.rapleaf.hank.part_daemon").setLevel(Level.INFO);
    Logger.getLogger("com.rapleaf.hank.storage").setLevel(Level.TRACE);
    create(domainsRoot);
    create(domainGroupsRoot);
    create(ringGroupsRoot);

    PrintWriter pw = new PrintWriter(new FileWriter(clientConfigYml));
    pw.println("---");
    pw.println("coordinator:");
    pw.println("  factory: com.rapleaf.hank.coordinator.zk.ZooKeeperCoordinator$Factory");
    pw.println("  options:");
    pw.println("    connect_string: localhost:" + getZkClientPort());
    pw.println("    session_timeout: 1000000");
    pw.println("    domains_root: " + domainsRoot);
    pw.println("    domain_groups_root: " + domainGroupsRoot);
    pw.println("    ring_groups_root: " + ringGroupsRoot);
    pw.close();

    pw = new PrintWriter(new FileWriter(domain0OptsYml));
    pw.println("---");
    pw.println("key_hash_size: 10");
    pw.println("hasher: " + Murmur64Hasher.class.getName());
    pw.println("max_allowed_part_size: " + 1024 * 1024);
    pw.println("hash_index_bits: 1");
    pw.println("record_file_read_buffer_bytes: 10240");
    pw.println("remote_domain_root: " + DOMAIN_0_DATAFILES);
    pw.println("file_ops_factory: " + LocalFileOps.Factory.class.getName());
    pw.close();
    AddDomain.main(new String[]{
        "--name", "domain0",
        "--num-parts", "2",
        "--storage-engine-factory", Curly.Factory.class.getName(),
        "--storage-engine-options", domain0OptsYml,
        "--partitioner", Murmur64Partitioner.class.getName(),
        "--config", clientConfigYml,
        "--initial-version", "1"});

    pw = new PrintWriter(new FileWriter(domain1OptsYml));
    pw.println("---");
    pw.println("key_hash_size: 10");
    pw.println("hasher: " + Murmur64Hasher.class.getName());
    pw.println("max_allowed_part_size: " + 1024 * 1024);
    pw.println("hash_index_bits: 1");
    pw.println("record_file_read_buffer_bytes: 10240");
    pw.println("remote_domain_root: " + DOMAIN_1_DATAFILES);
    pw.println("file_ops_factory: " + LocalFileOps.Factory.class.getName());
    pw.println("compression_codec: " + JavaGzipCompressionCodec.class.getName());
    pw.close();
    AddDomain.main(new String[]{
        "--name", "domain1",
        "--num-parts", "2",
        "--storage-engine-factory", Curly.Factory.class.getName(),
        "--storage-engine-options", domain1OptsYml,
        "--partitioner", Murmur64Partitioner.class.getName(),
        "--config", clientConfigYml,
        "--initial-version", "1"});

    Configurator config = new YamlClientConfigurator(clientConfigYml);

    Coordinator coord = config.getCoordinator();

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

    writeOut(coord.getDomainConfig("domain0"), domain0DataItems, 1, true, DOMAIN_0_DATAFILES);

    Map<ByteBuffer, ByteBuffer> domain1DataItems = new HashMap<ByteBuffer, ByteBuffer>();
    domain1DataItems.put(bb(4), bb(1, 1));
    domain1DataItems.put(bb(3), bb(2, 2));
    domain1DataItems.put(bb(2), bb(3, 3));
    domain1DataItems.put(bb(1), bb(4, 4));
    domain1DataItems.put(bb(8), bb(5, 1));
    domain1DataItems.put(bb(7), bb(6, 2));
    domain1DataItems.put(bb(6), bb(7, 3));
    domain1DataItems.put(bb(5), bb(8, 4));

    writeOut(coord.getDomainConfig("domain1"), domain1DataItems, 1, true, DOMAIN_1_DATAFILES);

    // configure domain group
    AddDomainGroup.main(new String[]{
       "--name", "dg1",
       "--config", clientConfigYml,
    });

    LOG.debug("-------- domain is created --------");

    // add our domains
    AddDomainToDomainGroup.main(new String[]{
        "--domain-group", "dg1",
        "--domain", "domain0",
        "--id", "0",
        "--config", clientConfigYml,
    });

    AddDomainToDomainGroup.main(new String[]{
        "--domain-group", "dg1",
        "--domain", "domain1",
        "--id", "1",
        "--config", clientConfigYml,
    });

    // simulate publisher pushing out a new version
    DomainGroupConfig domainGroupConfig = null;
    coord = config.getCoordinator();
    for (int i = 0; i < 15; i++) {
      domainGroupConfig = coord.getDomainGroupConfig("dg1");
      if (domainGroupConfig != null) {
        break;
      }
      Thread.sleep(1000);
    }
    assertNotNull("dg1 wasn't found, even after waiting 15 seconds!", domainGroupConfig);

    Map<String, Integer> versionMap = new HashMap<String, Integer>();
    versionMap.put("domain0", 1);
    versionMap.put("domain1", 1);
    domainGroupConfig.createNewVersion(versionMap);

    // configure ring group
    AddRingGroup.main(new String[]{
        "--ring-group", "rg1",
        "--domain-group", "dg1",
        "--config", clientConfigYml,
    });

    // add ring 1
    AddRing.main(new String[]{
        "--ring-group", "rg1",
        "--ring-number", "1",
        "--hosts", "localhost:50000,localhost:50001",
        "--config", clientConfigYml,
    });

    // add ring 2
    AddRing.main(new String[]{
        "--ring-group", "rg1",
        "--ring-number", "2",
        "--hosts", "localhost:50002,localhost:50003",
        "--config", clientConfigYml,
    });

    // launch 2x 2-node rings
    startDaemons(new PartDaemonAddress("localhost", 50000));
    startDaemons(new PartDaemonAddress("localhost", 50001));
    startDaemons(new PartDaemonAddress("localhost", 50002));
    startDaemons(new PartDaemonAddress("localhost", 50003));

    // launch the data deployer
    startDataDeployer();

//    Thread.sleep(100000000);

    // launch a smart client server
    startSmartClientServer();

    // open a dumb client (through the smart client)
    TTransport trans = new TFramedTransport(new TSocket("localhost", 50004));
    trans.open();
    TProtocol proto = new TCompactProtocol(trans);
    SmartClient.Client dumbClient = new SmartClient.Client(proto);

    boolean found = false;
    for (int i = 0; i < 15; i++) {
      HankResponse r = dumbClient.get("domain0", bb(1));
      if (r.isSet(_Fields.VALUE)) {
        found = true;
        break;
      }
      LOG.trace(r);
      Thread.sleep(1000);
    }

    if (!found) {
      fail("No ring came online in the time we waited!");
    }

    // make a few requests
    assertEquals(HankResponse.value(bb(1,1)), dumbClient.get("domain0", bb(1)));
    assertEquals(HankResponse.value(bb(2,2)), dumbClient.get("domain0", bb(2)));
    assertEquals(HankResponse.value(bb(3,3)), dumbClient.get("domain0", bb(3)));
    assertEquals(HankResponse.value(bb(4,4)), dumbClient.get("domain0", bb(4)));

    assertEquals(HankResponse.not_found(true), dumbClient.get("domain0", bb(99)));

    assertEquals(HankResponse.value(bb(1,1)), dumbClient.get("domain1", bb(4)));
    assertEquals(HankResponse.value(bb(2,2)), dumbClient.get("domain1", bb(3)));
    assertEquals(HankResponse.value(bb(3,3)), dumbClient.get("domain1", bb(2)));
    assertEquals(HankResponse.value(bb(4,4)), dumbClient.get("domain1", bb(1)));

    assertEquals(HankResponse.not_found(true), dumbClient.get("domain1", bb(99)));

    assertEquals(HankResponse.no_such_domain(true), dumbClient.get("domain2", bb(1)));

    // push a new version of one of the domains
    Map<ByteBuffer, ByteBuffer> domain1Delta = new HashMap<ByteBuffer, ByteBuffer>();
    domain1Delta.put(bb(4), bb(6, 6));
    domain1Delta.put(bb(5), bb(5, 5));

    writeOut(coord.getDomainConfig("domain1"), domain1Delta, 2, false, DOMAIN_1_DATAFILES);

    versionMap = new HashMap<String, Integer>();
    versionMap.put("domain0", 1);
    versionMap.put("domain1", 2);
    LOG.info("----- stamping new dg1 version -----");
    final DomainGroupConfigVersion newVersion = domainGroupConfig.createNewVersion(versionMap);

//    dumpZk();

    // wait until the rings have been updated to the new version
    coord = config.getCoordinator();
    final RingGroupConfig ringGroupConfig = coord.getRingGroupConfig("rg1");
    for (int i = 0; i < 30; i++) {
      if (ringGroupConfig.isUpdating()) {
        LOG.info("Ring group is still updating. Sleeping...");
      } else {
        if (ringGroupConfig.getCurrentVersion() == newVersion.getVersionNumber()) {
          break;
        } else {
          LOG.info("Ring group is not yet at the correct version. Continuing to wait.");
        }
      }
      Thread.sleep(1000);
    }

    assertFalse("ring group failed to finish updating after 30secs", ringGroupConfig.isUpdating());

    // keep making requests
    assertEquals(HankResponse.value(bb(1,1)), dumbClient.get("domain0", bb(1)));
    assertEquals(HankResponse.value(bb(2,2)), dumbClient.get("domain0", bb(2)));
    assertEquals(HankResponse.value(bb(3,3)), dumbClient.get("domain0", bb(3)));
    assertEquals(HankResponse.value(bb(4,4)), dumbClient.get("domain0", bb(4)));

    assertEquals(HankResponse.not_found(true), dumbClient.get("domain0", bb(99)));

    assertEquals(HankResponse.value(bb(6,6)), dumbClient.get("domain1", bb(4)));
    assertEquals(HankResponse.value(bb(2,2)), dumbClient.get("domain1", bb(3)));
    assertEquals(HankResponse.value(bb(3,3)), dumbClient.get("domain1", bb(2)));
    assertEquals(HankResponse.value(bb(4,4)), dumbClient.get("domain1", bb(1)));
    assertEquals(HankResponse.value(bb(5,5)), dumbClient.get("domain1", bb(5)));

    assertEquals(HankResponse.no_such_domain(true), dumbClient.get("domain2", bb(1)));

    // take down one of the nodes in one of the rings "unexpectedly"
    stopDaemons(new PartDaemonAddress("localhost", 50000));
    stopDaemons(new PartDaemonAddress("localhost", 50001));

    // keep making requests
    assertEquals(HankResponse.value(bb(1,1)), dumbClient.get("domain0", bb(1)));
    assertEquals(HankResponse.value(bb(2,2)), dumbClient.get("domain0", bb(2)));
    assertEquals(HankResponse.value(bb(3,3)), dumbClient.get("domain0", bb(3)));
    assertEquals(HankResponse.value(bb(4,4)), dumbClient.get("domain0", bb(4)));

    assertEquals(HankResponse.not_found(true), dumbClient.get("domain0", bb(99)));

    assertEquals(HankResponse.value(bb(6,6)), dumbClient.get("domain1", bb(4)));
    assertEquals(HankResponse.value(bb(2,2)), dumbClient.get("domain1", bb(3)));
    assertEquals(HankResponse.value(bb(3,3)), dumbClient.get("domain1", bb(2)));
    assertEquals(HankResponse.value(bb(4,4)), dumbClient.get("domain1", bb(1)));
    assertEquals(HankResponse.value(bb(5,5)), dumbClient.get("domain1", bb(5)));

    assertEquals(HankResponse.no_such_domain(true), dumbClient.get("domain2", bb(1)));

    // shut it all down
    stopDataDeployer();
    stopSmartClient();

    stopDaemons(new PartDaemonAddress("localhost", 50002));
    stopDaemons(new PartDaemonAddress("localhost", 50003));
  }

  private void startSmartClientServer() throws Exception {
    LOG.debug("starting smart client server...");
    smartClientRunnable = new SmartClientRunnable();
//    smartClientThread = new Thread(smartClientRunnable, "smart client server thread");
//    smartClientThread.start();
    smartClientRunnable.run();
  }

  private void stopSmartClient() throws Exception {
    smartClientRunnable.pleaseStop();
  }

  private void startDataDeployer() throws Exception {
    LOG.debug("starting data deployer");
    dataDeployerRunnable = new DataDeployerRunnable();
    dataDeployerThread = new Thread(dataDeployerRunnable, "data deployer thread");
    dataDeployerThread.start();
  }

  private void stopDataDeployer() throws Exception {
    LOG.debug("stopping data deployer");
    dataDeployerRunnable.pleaseStop();
    dataDeployerThread.join();
  }

  private void startDaemons(PartDaemonAddress a) throws Exception {
    LOG.debug("Starting daemons for " + a);
    PartDaemonRunnable pr = new PartDaemonRunnable(a);
    partDaemonRunnables.put(a, pr);
    Thread pt = new Thread(pr, "part daemon thread for " + a);
    partDaemonThreads.put(a, pt);
    pt.start();
  }

  private void stopDaemons(PartDaemonAddress a) throws Exception {
    LOG.debug("Stopping daemons for " + a);
    partDaemonRunnables.get(a).pleaseStop();
    partDaemonThreads.get(a).join();
  }

  private void writeOut(final DomainConfig domainConfig, Map<ByteBuffer, ByteBuffer> dataItems, int versionNumber, boolean isBase, String domainRoot) throws IOException {
    // partition keys and values
    Map<Integer, SortedMap<ByteBuffer, ByteBuffer>> sortedAndPartitioned = new HashMap<Integer, SortedMap<ByteBuffer,ByteBuffer>>();
    Partitioner p = domainConfig.getPartitioner();
    for (Map.Entry<ByteBuffer, ByteBuffer> pair : dataItems.entrySet()) {
      int partNum = p.partition(pair.getKey()) % domainConfig.getNumParts();
      SortedMap<ByteBuffer, ByteBuffer> part = sortedAndPartitioned.get(partNum);
      if (part == null) {
        part = new TreeMap<ByteBuffer, ByteBuffer>(new Comparator<ByteBuffer>() {
          @Override
          public int compare(ByteBuffer arg0, ByteBuffer arg1) {
            final StorageEngine storageEngine = domainConfig.getStorageEngine();
            final ByteBuffer keyL = storageEngine.getComparableKey(arg0);
            final ByteBuffer keyR = storageEngine.getComparableKey(arg1);
            return Bytes.compareBytes(keyL.array(), keyL.position(), keyR.array(), keyR.position(), keyL.limit());
          }
        });
        sortedAndPartitioned.put(partNum, part);
      }
      part.put(pair.getKey(), pair.getValue());
    }

    StorageEngine engine = domainConfig.getStorageEngine();
    new File(domainRoot).mkdirs();
    for (Map.Entry<Integer, SortedMap<ByteBuffer, ByteBuffer>> part : sortedAndPartitioned.entrySet()) {
      LOG.debug("Writing out part " + part.getKey() + " for domain " + domainConfig.getName() + " to root " + domainRoot);
      Writer writer = engine.getWriter(new LocalDiskOutputStreamFactory(domainRoot), part.getKey(), versionNumber, isBase);
      final SortedMap<ByteBuffer, ByteBuffer> partPairs = part.getValue();
      for (Map.Entry<ByteBuffer, ByteBuffer> pair : partPairs.entrySet()) {
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
