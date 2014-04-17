package com.liveramp.hank;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.liveramp.hank.client.HankSmartClient;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.coordinator.zk.ZooKeeperCoordinator;
import com.liveramp.hank.generated.ClientMetadata;
import com.liveramp.hank.partitioner.Murmur64Partitioner;
import com.liveramp.hank.storage.echo.Echo;
import com.liveramp.hank.test.ZkTestCase;
import com.liveramp.hank.util.LocalHostUtils;
import com.liveramp.hank.zookeeper.ZkPath;

public abstract class ZkMockCoordinatorTestCase extends ZkTestCase {

  public static final String DOMAIN_GROUP_0 = "domain-group-0";
  public static final String DOMAIN_GROUP_1 = "domain-group-1";

  public static final String DOMAIN_0 = "domain-0";
  public static final String DOMAIN_1 = "domain-1";

  public static final String RING_GROUP_0 = "ring-group-0";
  public static final String RING_GROUP_1 = "ring-group-1";
  public static final String RING_GROUP_2 = "ring-group-2";

  protected Coordinator getMockCoordinator() throws Exception {

    return new ZooKeeperCoordinator.Factory().getCoordinator(
        ZooKeeperCoordinator.Factory.requiredOptions(getZkConnectString(), 100000000,
            ZkPath.append(getRoot(), "domains"),
            ZkPath.append(getRoot(), "domain_groups"),
            ZkPath.append(getRoot(), "ring_groups")));
  }

  protected Coordinator getApiMockCoordinator() throws Exception {
    Coordinator coord = getMockCoordinator();

    String d0Conf = "---\n  blah: blah\n  moreblah: blahblah";

    final Domain d0 = coord.addDomain(DOMAIN_0, 32, Echo.Factory.class.getName(), d0Conf, Murmur64Partitioner.class.getName(), Collections.<String>emptyList());
    DomainVersion ver = d0.openNewVersion(null);
    ver.close();
    ver = d0.openNewVersion(null);
    final Domain d1 = coord.addDomain(DOMAIN_1, 32, Echo.Factory.class.getName(), "---", Murmur64Partitioner.class.getName(), Collections.<String>emptyList());
    ver = d1.openNewVersion(null);
    dumpZk();
    ver.close();
    ver = d1.openNewVersion(null);
    ver.close();

    DomainGroup g1 = coord.addDomainGroup(DOMAIN_GROUP_0);
    Map<Domain, Integer> g1Versions = new HashMap<Domain, Integer>();
    g1Versions.put(d0, 1);
    g1Versions.put(d1, 1);
    g1.setDomainVersions(g1Versions);

    DomainGroup g2 = coord.addDomainGroup(DOMAIN_GROUP_1);
    Map<Domain, Integer> g2Versions = new HashMap<Domain, Integer>();
    g2Versions.put(d1, 1);
    g2.setDomainVersions(g2Versions);

    RingGroup rg0 = coord.addRingGroup(RING_GROUP_0, g1.getName());
    Ring r1 = rg0.addRing(1);
    r1.addHost(addy("alpha-1-1"), Collections.<String>emptyList());
    r1.addHost(addy("alpha-1-2"), Collections.<String>emptyList());
    r1.addHost(addy("alpha-1-3"), Collections.<String>emptyList());
    Ring r2 = rg0.addRing(2);
    r2.addHost(addy("alpha-2-1"), Collections.<String>emptyList());
    r2.addHost(addy("alpha-2-2"), Collections.<String>emptyList());
    r2.addHost(addy("alpha-2-3"), Collections.<String>emptyList());
    Ring r3 = rg0.addRing(3);
    r3.addHost(addy("alpha-3-1"), Collections.<String>emptyList());
    r3.addHost(addy("alpha-3-2"), Collections.<String>emptyList());
    r3.addHost(addy("alpha-3-3"), Collections.<String>emptyList());
    for (int i = 0; i < 16; ++i) {
      rg0.registerClient(new ClientMetadata(
          LocalHostUtils.getHostName(),
          System.currentTimeMillis(),
          HankSmartClient.class.getName(),
          Hank.getGitCommit()));
    }

    RingGroup rg1 = coord.addRingGroup(RING_GROUP_1, g1.getName());
    r1 = rg1.addRing(1);
    r1.addHost(addy("beta-1-1"), Collections.<String>emptyList());
    r1.addHost(addy("beta-1-2"), Collections.<String>emptyList());
    r1.addHost(addy("beta-1-3"), Collections.<String>emptyList());
    r1.addHost(addy("beta-1-4"), Collections.<String>emptyList());
    r2 = rg1.addRing(2);
    r2.addHost(addy("beta-2-1"), Collections.<String>emptyList());
    r2.addHost(addy("beta-2-2"), Collections.<String>emptyList());
    r2.addHost(addy("beta-2-3"), Collections.<String>emptyList());
    r2.addHost(addy("beta-2-4"), Collections.<String>emptyList());
    r3 = rg1.addRing(3);
    r3.addHost(addy("beta-3-1"), Collections.<String>emptyList());
    r3.addHost(addy("beta-3-2"), Collections.<String>emptyList());
    r3.addHost(addy("beta-3-3"), Collections.<String>emptyList());
    r3.addHost(addy("beta-3-4"), Collections.<String>emptyList());
    Ring r4 = rg1.addRing(4);
    r4.addHost(addy("beta-4-1"), Collections.<String>emptyList());
    r4.addHost(addy("beta-4-2"), Collections.<String>emptyList());
    r4.addHost(addy("beta-4-3"), Collections.<String>emptyList());
    r4.addHost(addy("beta-4-4"), Collections.<String>emptyList());

    RingGroup rg2 = coord.addRingGroup(RING_GROUP_2, g2.getName());
    r1 = rg2.addRing(1);
    r1.addHost(addy("gamma-1-1"), Collections.<String>emptyList());

    return coord;
  }

  private PartitionServerAddress addy(String hostname) {
    return new PartitionServerAddress(hostname, 6200);
  }
}
