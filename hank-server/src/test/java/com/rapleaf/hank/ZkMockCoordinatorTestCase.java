package com.rapleaf.hank;

import com.rapleaf.hank.client.HankSmartClient;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.coordinator.zk.ZooKeeperCoordinator;
import com.rapleaf.hank.generated.ClientMetadata;
import com.rapleaf.hank.partitioner.Murmur64Partitioner;
import com.rapleaf.hank.storage.echo.Echo;
import com.rapleaf.hank.test.ZkTestCase;
import com.rapleaf.hank.util.LocalHostUtils;
import com.rapleaf.hank.zookeeper.ZkPath;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class ZkMockCoordinatorTestCase extends ZkTestCase {

  protected Coordinator getMockCoordinator() throws Exception {
    create(ZkPath.append(getRoot(), "domains"));
    create(ZkPath.append(getRoot(), "domain_groups"));
    create(ZkPath.append(getRoot(), "ring_groups"));

    final Coordinator coord = new ZooKeeperCoordinator.Factory().getCoordinator(
        ZooKeeperCoordinator.Factory.requiredOptions(getZkConnectString(), 100000000,
            ZkPath.append(getRoot(), "domains"),
            ZkPath.append(getRoot(), "domain_groups"),
            ZkPath.append(getRoot(), "ring_groups")));

    String d0Conf = "---\n  blah: blah\n  moreblah: blahblah";

    final Domain d0 = coord.addDomain("domain0", 32, Echo.Factory.class.getName(), d0Conf, Murmur64Partitioner.class.getName(), Collections.<String>emptyList());
    DomainVersion ver = d0.openNewVersion(null);
    ver.close();
    ver = d0.openNewVersion(null);
    final Domain d1 = coord.addDomain("domain1", 32, Echo.Factory.class.getName(), "---", Murmur64Partitioner.class.getName(), Collections.<String>emptyList());
    ver = d1.openNewVersion(null);
    dumpZk();
    ver.close();
    ver = d1.openNewVersion(null);
    ver.close();

    DomainGroup g1 = coord.addDomainGroup("Group_1");
    Map<Domain, Integer> g1Versions = new HashMap<Domain, Integer>();
    g1Versions.put(d0, 1);
    g1Versions.put(d1, 1);
    g1.setDomainVersions(g1Versions);

    DomainGroup g2 = coord.addDomainGroup("Group_2");
    Map<Domain, Integer> g2Versions = new HashMap<Domain, Integer>();
    g2Versions.put(d1, 1);
    g2.setDomainVersions(g2Versions);

    RingGroup rgAlpha = coord.addRingGroup("RG_Alpha", g1.getName());
    Ring r1 = rgAlpha.addRing(1);
    r1.addHost(addy("alpha-1-1"), Collections.<String>emptyList());
    r1.addHost(addy("alpha-1-2"), Collections.<String>emptyList());
    r1.addHost(addy("alpha-1-3"), Collections.<String>emptyList());
    Ring r2 = rgAlpha.addRing(2);
    r2.addHost(addy("alpha-2-1"), Collections.<String>emptyList());
    r2.addHost(addy("alpha-2-2"), Collections.<String>emptyList());
    r2.addHost(addy("alpha-2-3"), Collections.<String>emptyList());
    Ring r3 = rgAlpha.addRing(3);
    r3.addHost(addy("alpha-3-1"), Collections.<String>emptyList());
    r3.addHost(addy("alpha-3-2"), Collections.<String>emptyList());
    r3.addHost(addy("alpha-3-3"), Collections.<String>emptyList());
    for (int i = 0; i < 16; ++i) {
      rgAlpha.registerClient(new ClientMetadata(
          LocalHostUtils.getHostName(),
          System.currentTimeMillis(),
          HankSmartClient.class.getName(),
          Hank.getGitCommit()));
    }

    RingGroup rgBeta = coord.addRingGroup("RG_Beta", g1.getName());
    r1 = rgBeta.addRing(1);
    r1.addHost(addy("beta-1-1"), Collections.<String>emptyList());
    r1.addHost(addy("beta-1-2"), Collections.<String>emptyList());
    r1.addHost(addy("beta-1-3"), Collections.<String>emptyList());
    r1.addHost(addy("beta-1-4"), Collections.<String>emptyList());
    r2 = rgBeta.addRing(2);
    r2.addHost(addy("beta-2-1"), Collections.<String>emptyList());
    r2.addHost(addy("beta-2-2"), Collections.<String>emptyList());
    r2.addHost(addy("beta-2-3"), Collections.<String>emptyList());
    r2.addHost(addy("beta-2-4"), Collections.<String>emptyList());
    r3 = rgBeta.addRing(3);
    r3.addHost(addy("beta-3-1"), Collections.<String>emptyList());
    r3.addHost(addy("beta-3-2"), Collections.<String>emptyList());
    r3.addHost(addy("beta-3-3"), Collections.<String>emptyList());
    r3.addHost(addy("beta-3-4"), Collections.<String>emptyList());
    Ring r4 = rgBeta.addRing(4);
    r4.addHost(addy("beta-4-1"), Collections.<String>emptyList());
    r4.addHost(addy("beta-4-2"), Collections.<String>emptyList());
    r4.addHost(addy("beta-4-3"), Collections.<String>emptyList());
    r4.addHost(addy("beta-4-4"), Collections.<String>emptyList());

    RingGroup rgGamma = coord.addRingGroup("RG_Gamma", g2.getName());
    r1 = rgGamma.addRing(1);
    r1.addHost(addy("gamma-1-1"), Collections.<String>emptyList());

    return coord;
  }

  private PartitionServerAddress addy(String hostname) {
    return new PartitionServerAddress(hostname, 6200);
  }
}
