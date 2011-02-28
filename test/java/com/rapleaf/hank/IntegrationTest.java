package com.rapleaf.hank;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

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
import com.rapleaf.hank.config.YamlConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.SmartClient;
import com.rapleaf.hank.partitioner.Murmur64Partitioner;
import com.rapleaf.hank.storage.curly.Curly;

public class IntegrationTest extends ZkTestCase {
  public void testItAll() throws Throwable {
    // initialize the bare structure
    create(getRoot() + "/domains");
    create(getRoot() + "/domain_groups");
    create(getRoot() + "/ring_groups");

    PrintWriter pw = new PrintWriter(new FileWriter(localTmpDir + "/config.yml"));
    pw.println("---");
    pw.println("coordinator:");
    pw.println("  factory: com.rapleaf.hank.coordinator.zk.ZooKeeperCoordinator$Factory");
    pw.println("  options:");
    pw.println("    connect_string: localhost:" + getZkClientPort());
    pw.println("    session_timeout: 1000000");
    pw.println("    domains_root: " + getRoot() + "/domains");
    pw.println("    domain_groups_root: " + getRoot() + "/domain_groups");
    pw.println("    ring_groups_root: " + getRoot() + "/ring_groups");
    pw.close();

    // use cli tools to create a pair of domains
    // TODO: set up domain0_opts.yml
    AddDomain.main(new String[]{
        "--name", "domain0",
        "--num-parts", "2",
        "--storage-engine-factory", Curly.Factory.class.getName(),
        "--storage-engine-options", "domain0_opts.yml",
        "--partitioner", Murmur64Partitioner.class.getName(),
        "--config", localTmpDir + "/config.yml",
        "--initial-version", "1"});

    // TODO: set up domain1_opts.yml
    AddDomain.main(new String[]{
        "--name", "domain1",
        "--num-parts", "2",
        "--storage-engine-factory", Curly.Factory.class.getName(),
        "--storage-engine-options", "domain1_opts.yml",
        "--partitioner", Murmur64Partitioner.class.getName(),
        "--initial-version", "1"});

    // TODO: set up yaml config for clientside
    String configPath = null;
    Configurator config = new YamlConfigurator(configPath);

    Coordinator coord = config.getCoordinator();

    // write a base version of each domain
    Map<ByteBuffer, ByteBuffer> domain0DataItems = new HashMap<ByteBuffer, ByteBuffer>();
    domain0DataItems.put(bb(1), bb(1, 1));
    domain0DataItems.put(bb(2), bb(2, 2));
    domain0DataItems.put(bb(3), bb(3, 3));
    domain0DataItems.put(bb(4), bb(4, 4));

    writeOut(coord.getDomainConfig("domain0"), domain0DataItems);

    Map<ByteBuffer, ByteBuffer> domain1DataItems = new HashMap<ByteBuffer, ByteBuffer>();
    domain1DataItems.put(bb(4), bb(1, 1));
    domain1DataItems.put(bb(3), bb(2, 2));
    domain1DataItems.put(bb(2), bb(3, 3));
    domain1DataItems.put(bb(1), bb(4, 4));

    writeOut(coord.getDomainConfig("domain1"), domain1DataItems);

    // configure domain group
    AddDomainGroup.main(new String[]{
       "--name", getRoot() + "/domain_groups/dg1"
    });

    // add our domains
    AddDomainToDomainGroup.main(new String[]{
        "--domain-group", getRoot() + "/domain_groups/dg1",
        "--domain", "domain0",
        "--domain-id", "0"
    });

    AddDomainToDomainGroup.main(new String[]{
        "--domain-group", getRoot() + "/domain_groups/dg1",
        "--domain", "domain1",
        "--domain-id", "1"
    });

    // simulate publisher pushing out a new version
    DomainGroupConfig domainGroupConfig = coord.getDomainGroupConfig("dg1");
    domainGroupConfig.addDomain(coord.getDomainConfig("domain0"), 0);
    domainGroupConfig.addDomain(coord.getDomainConfig("domain1"), 1);

    // configure ring group
    AddRingGroup.main(new String[]{
        "--name", getRoot() + "/ring_groups/rg1",
        "--domain-group", getRoot() + "/domain_groups/dg1",
        "--initial-version", "" + domainGroupConfig.getLatestVersion().getVersionNumber()
    });

    // add ring 1
    AddRing.main(new String[]{
        "--ring-group", getRoot() + "/ring_groups/rg1",
        "--num", "1",
        "--hosts", "localhost:50000,localhost:50001"
    });

    // add ring 2
    AddRing.main(new String[]{
        "--ring-group", getRoot() + "/ring_groups/rg1",
        "--num", "2",
        "--hosts", "localhost:50002,localhost:50003"
    });

    // launch 2x 2-node rings
    startDaemons(new PartDaemonAddress("localhost", 50000));
    startDaemons(new PartDaemonAddress("localhost", 50001));
    startDaemons(new PartDaemonAddress("localhost", 50002));
    startDaemons(new PartDaemonAddress("localhost", 50003));

    // launch the data deployer
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

    writeOut(coord.getDomainConfig("domain1"), domain1Delta);

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

  private void stopDaemons(PartDaemonAddress partDaemonAddress) {
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

  private void startDaemons(PartDaemonAddress partDaemonAddress) {
    fail("not implemented");
  }

  private void writeOut(DomainConfig domainConfig, Map<ByteBuffer, ByteBuffer> dataItems) {
    fail();
  }

  private static ByteBuffer bb(int... bs) {
    byte[] bytes = new byte[bs.length];
    for (int i = 0; i < bs.length; i++) {
      bytes[i] = (byte) bs[i];
    }
    return ByteBuffer.wrap(bytes);
  }
}
