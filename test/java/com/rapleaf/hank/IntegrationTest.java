package com.rapleaf.hank;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

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
import com.rapleaf.hank.config.Configurator;
import com.rapleaf.hank.config.PartDaemonConfigurator;
import com.rapleaf.hank.config.UpdateDaemonConfigurator;
import com.rapleaf.hank.config.YamlConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.SmartClient;
import com.rapleaf.hank.hasher.Murmur64Hasher;
import com.rapleaf.hank.partitioner.Murmur64Partitioner;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.storage.curly.Curly;

public class IntegrationTest extends ZkTestCase {
  private final class UpdateDaemonRunnable implements Runnable {
    private String configPath;
    private com.rapleaf.hank.update_daemon.UpdateDaemon server;
    public Throwable throwable;

    public UpdateDaemonRunnable(PartDaemonAddress addy) throws Exception {
      String hostDotPort = addy.getHostName() + "." + addy.getPortNumber();
      this.configPath = localTmpDir + "/" + hostDotPort + ".part_daemon.yml";

      PrintWriter pw = new PrintWriter(new FileWriter(configPath));
      pw.println("---");
      pw.println("part_daemon:");
      pw.println("  service_port: " + addy.getPortNumber());
      pw.println("ring_group_name: rg1");
      pw.println("local_data_dirs:");
      pw.println("  - " + localTmpDir + "/" + hostDotPort);
      pw.println("update_daemon:");
      pw.println("  num_concurrent_updates: 1");
      pw.println("coordinator:");
      pw.println("  factory: com.rapleaf.hank.coordinator.zk.ZooKeeperCoordinator$Factory");
      pw.println("  options:");
      pw.println("    connect_string: localhost:" + getZkClientPort());
      pw.println("    session_timeout: 1000000");
      pw.println("    domains_root: " + domainsRoot);
      pw.println("    domain_groups_root: " + domainGroupsRoot);
      pw.println("    ring_groups_root: " + ringGroupsRoot);
      pw.close();
    }

    @Override
    public void run() {
      try {
        UpdateDaemonConfigurator c = new YamlConfigurator(configPath);
        server = new com.rapleaf.hank.update_daemon.UpdateDaemon(c, "localhost");
        server.run();
      } catch (Throwable t) {
        LOG.fatal("crap, some error...", t);
        throwable = t;
      }
    }

    public void pleaseStop() {
      server.stop();
    }

  }

  private final class PartDaemonRunnable implements Runnable {
    private final PartDaemonAddress partDaemonAddress;
    private final String configPath;
    private Throwable throwable;
    private com.rapleaf.hank.part_daemon.Server server;

    public PartDaemonRunnable(PartDaemonAddress addy) throws IOException {
      this.partDaemonAddress = addy;
      String hostDotPort = addy.getHostName()
                + "." + addy.getPortNumber();
      this.configPath = localTmpDir + "/" + hostDotPort + ".part_daemon.yml";

      PrintWriter pw = new PrintWriter(new FileWriter(configPath));
      pw.println("---");
      pw.println("part_daemon:");
      pw.println("  service_port: " + addy.getPortNumber());
      pw.println("ring_group_name: rg1");
      pw.println("local_data_dirs:");
      pw.println("  - " + localTmpDir + "/" + hostDotPort);
      pw.println("update_daemon:");
      pw.println("  num_concurrent_updates: 1");
      pw.println("coordinator:");
      pw.println("  factory: com.rapleaf.hank.coordinator.zk.ZooKeeperCoordinator$Factory");
      pw.println("  options:");
      pw.println("    connect_string: localhost:" + getZkClientPort());
      pw.println("    session_timeout: 1000000");
      pw.println("    domains_root: " + domainsRoot);
      pw.println("    domain_groups_root: " + domainGroupsRoot);
      pw.println("    ring_groups_root: " + ringGroupsRoot);
      pw.close();
    }

    @Override
    public void run() {
      try {
        PartDaemonConfigurator configurator = new YamlConfigurator(configPath);
        server = new com.rapleaf.hank.part_daemon.Server(configurator, "localhost");
        server.run();
      } catch (Throwable t) {
        LOG.fatal("crap, some exception...", t);
        throwable = t;
      }
    }

    public void pleaseStop() {
      server.stopServer();
    }

  }

  private static final Logger LOG = Logger.getLogger(IntegrationTest.class);

  private static class LocalDiskOutputStreamFactory implements OutputStreamFactory {
    private final String basePath;

    public LocalDiskOutputStreamFactory(String basePath) {
      this.basePath = basePath;
    }

    @Override
    public OutputStream getOutputStream(int partNum, String name) throws IOException {
      String fullPath = basePath + "/" + partNum + "/" + name;
      new File(new File(fullPath).getParent()).mkdirs();
      return new FileOutputStream(fullPath);
    }
  }

  private final String domainsRoot = getRoot() + "/domains";
  private final String domainGroupsRoot = getRoot() + "/domain_groups";
  private final String ringGroupsRoot = getRoot() + "/ring_groups";
  private final String clientConfigYml = localTmpDir + "/config.yml";
  private final String domain0OptsYml = localTmpDir + "/domain0_opts.yml";
  private final String domain1OptsYml = localTmpDir + "/domain1_opts.yml";
  private final String localTmpDomains = localTmpDir + "/domain_persistence";
  private final Map<PartDaemonAddress, Thread> partDaemonThreads = new HashMap<PartDaemonAddress, Thread>();
  private final Map<PartDaemonAddress, PartDaemonRunnable> partDaemonRunnables = new HashMap<PartDaemonAddress, PartDaemonRunnable>();
  private final Map<PartDaemonAddress, Thread> updateDaemonThreads = new HashMap<PartDaemonAddress, Thread>();
  private final Map<PartDaemonAddress, UpdateDaemonRunnable> updateDaemonRunnables = new HashMap<PartDaemonAddress, UpdateDaemonRunnable>();

  public void testItAll() throws Throwable {
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
    pw.println("hash_index_bits: 10");
    pw.println("cueball_read_buffer_bytes: 10240");
    pw.println("record_file_read_buffer_bytes: 10240");
    pw.println("remote_domain_root: /tmp/domain0_datafiles");
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
    pw.println("hash_index_bits: 10");
    pw.println("cueball_read_buffer_bytes: 10240");
    pw.println("record_file_read_buffer_bytes: 10240");
    pw.println("remote_domain_root: /tmp/domain1_datafiles");
    pw.close();
    AddDomain.main(new String[]{
        "--name", "domain1",
        "--num-parts", "2",
        "--storage-engine-factory", Curly.Factory.class.getName(),
        "--storage-engine-options", domain1OptsYml,
        "--partitioner", Murmur64Partitioner.class.getName(),
        "--config", clientConfigYml,
        "--initial-version", "1"});

    Configurator config = new YamlConfigurator(clientConfigYml);

    Coordinator coord = config.getCoordinator();

    // write a base version of each domain
    Map<ByteBuffer, ByteBuffer> domain0DataItems = new HashMap<ByteBuffer, ByteBuffer>();
    domain0DataItems.put(bb(1), bb(1, 1));
    domain0DataItems.put(bb(2), bb(2, 2));
    domain0DataItems.put(bb(3), bb(3, 3));
    domain0DataItems.put(bb(4), bb(4, 4));

    writeOut(coord.getDomainConfig("domain0"), domain0DataItems, 1, true);

    Map<ByteBuffer, ByteBuffer> domain1DataItems = new HashMap<ByteBuffer, ByteBuffer>();
    domain1DataItems.put(bb(4), bb(1, 1));
    domain1DataItems.put(bb(3), bb(2, 2));
    domain1DataItems.put(bb(2), bb(3, 3));
    domain1DataItems.put(bb(1), bb(4, 4));

    writeOut(coord.getDomainConfig("domain1"), domain1DataItems, 1, true);

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
//    Thread.sleep(1000000);
    startDataDeployer();

    // launch a smart client server
    startSmartClientServer();

    // open a dumb client (through the smart client)
    TTransport trans = new TFramedTransport(new TSocket("localhost", 50004));
    TProtocol proto = new TCompactProtocol(trans);
    SmartClient.Client dumbClient = new SmartClient.Client(proto);

    // make a few requests
    assertEquals(HankResponse.value(bb(1,1)), dumbClient.get("domain0", bb(1)));
    assertEquals(HankResponse.value(bb(2,2)), dumbClient.get("domain0", bb(2)));
    assertEquals(HankResponse.value(bb(3,3)), dumbClient.get("domain0", bb(3)));
    assertEquals(HankResponse.value(bb(4,4)), dumbClient.get("domain0", bb(4)));

    assertEquals(HankResponse.not_found(true), dumbClient.get("domain0", bb(5)));

    assertEquals(HankResponse.value(bb(1,1)), dumbClient.get("domain1", bb(4)));
    assertEquals(HankResponse.value(bb(2,2)), dumbClient.get("domain1", bb(3)));
    assertEquals(HankResponse.value(bb(3,3)), dumbClient.get("domain1", bb(2)));
    assertEquals(HankResponse.value(bb(4,4)), dumbClient.get("domain1", bb(1)));

    assertEquals(HankResponse.not_found(true), dumbClient.get("domain1", bb(5)));

    assertEquals(HankResponse.no_such_domain(true), dumbClient.get("domain2", bb(1)));

    // push a new version of one of the domains
    Map<ByteBuffer, ByteBuffer> domain1Delta = new HashMap<ByteBuffer, ByteBuffer>();
    domain1Delta.put(bb(4), bb(6, 6));
    domain1Delta.put(bb(5), bb(5, 5));

    writeOut(coord.getDomainConfig("domain1"), domain1Delta, 2, false);

    // wait until the rings have been updated to the new version
    fail("not implemented");

    // keep making requests
    assertEquals(HankResponse.value(bb(1,1)), dumbClient.get("domain0", bb(1)));
    assertEquals(HankResponse.value(bb(2,2)), dumbClient.get("domain0", bb(2)));
    assertEquals(HankResponse.value(bb(3,3)), dumbClient.get("domain0", bb(3)));
    assertEquals(HankResponse.value(bb(4,4)), dumbClient.get("domain0", bb(4)));

    assertEquals(HankResponse.not_found(true), dumbClient.get("domain0", bb(5)));

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

    assertEquals(HankResponse.not_found(true), dumbClient.get("domain0", bb(5)));

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

  private void stopSmartClient() {
    fail("not implemented");
  }

  private void stopDataDeployer() {
    fail("not implemented");
  }

  private void startSmartClientServer() {
    fail("not implemented");
  }

  private void startDataDeployer() {
    fail("not implemented");
  }

  private void startDaemons(PartDaemonAddress a) throws Exception {
    PartDaemonRunnable pr = new PartDaemonRunnable(a);
    partDaemonRunnables.put(a, pr);
    Thread pt = new Thread(pr, "part daemon thread for " + a);
    partDaemonThreads.put(a, pt);
    pt.start();
    UpdateDaemonRunnable ur = new UpdateDaemonRunnable(a);
    updateDaemonRunnables.put(a, ur);
    Thread ut = new Thread(ur, "update daemon thread for " + a);
    updateDaemonThreads.put(a, ut);
    ut.start();
  }

  private void stopDaemons(PartDaemonAddress a) throws Exception {
    partDaemonRunnables.get(a).pleaseStop();
    updateDaemonRunnables.get(a).pleaseStop();
    partDaemonThreads.get(a).join();
    updateDaemonThreads.get(a).join();
  }

  private void writeOut(DomainConfig domainConfig, Map<ByteBuffer, ByteBuffer> dataItems, int versionNumber, boolean isBase) throws IOException {
    // partition keys and values
    Map<Integer, SortedMap<ByteBuffer, ByteBuffer>> sortedAndPartitioned = new HashMap<Integer, SortedMap<ByteBuffer,ByteBuffer>>();
    Partitioner p = domainConfig.getPartitioner();
    for (Map.Entry<ByteBuffer, ByteBuffer> pair : dataItems.entrySet()) {
      int partNum = p.partition(pair.getKey()) % domainConfig.getNumParts();
      SortedMap<ByteBuffer, ByteBuffer> part = sortedAndPartitioned.get(partNum);
      if (part == null) {
        part = new TreeMap<ByteBuffer, ByteBuffer>();
        sortedAndPartitioned.put(partNum, part);
      }
      part.put(pair.getKey(), pair.getValue());
    }

    StorageEngine engine = domainConfig.getStorageEngine();
    for (Map.Entry<Integer, SortedMap<ByteBuffer, ByteBuffer>> part : sortedAndPartitioned.entrySet()) {
      Writer writer = engine.getWriter(new LocalDiskOutputStreamFactory(localTmpDomains + "/" + domainConfig.getName()), part.getKey(), versionNumber, isBase);
      for (Map.Entry<ByteBuffer, ByteBuffer> pair : part.getValue().entrySet()) {
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
}
