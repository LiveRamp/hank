package com.rapleaf.hank.ui;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.config.ClientConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.coordinator.zk.ZooKeeperCoordinator;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.SmartClient.Iface;
import com.rapleaf.hank.partitioner.Murmur64Partitioner;
import com.rapleaf.hank.storage.curly.Curly;

public class StatusWebDaemonTester extends ZkTestCase {
  public void testIt() throws Exception {
    create(getRoot() + "/domains");
    create(getRoot() + "/domain_groups");
    create(getRoot() + "/ring_groups");
    final Coordinator coord = new ZooKeeperCoordinator.Factory().getCoordinator(
        ZooKeeperCoordinator.Factory.requiredOptions(getZkConnectString(), 100000000, getRoot()
        + "/domains", getRoot() + "/domain_groups", getRoot() + "/ring_groups"));

    String d0Conf = "---\n  blah: blah\n  moreblah: blahblah";

    final Domain d0 = coord.addDomain("domain0", 1024, Curly.Factory.class.getName(), d0Conf, Murmur64Partitioner.class.getName());
    DomainVersion ver = d0.openNewVersion();
    ver.close();
    ver = d0.openNewVersion();
    ver.addPartitionInfo(0, 1024, 55);
    final Domain d1 = coord.addDomain("domain1", 1024, Curly.Factory.class.getName(), "---", Murmur64Partitioner.class.getName());
    ver = d1.openNewVersion();
    dumpZk();
    ver.addPartitionInfo(0, 1024, 55);
    ver.addPartitionInfo(1, 32555, 7500000000L);
    ver.close();
    ver = d1.openNewVersion();
    ver.close();

    DomainGroup g1 = coord.addDomainGroup("Group_1");
    g1.addDomain(d0, 0);
    g1.addDomain(d1, 1);

    g1.createNewVersion(new HashMap<String, Integer>() {
      {
        put(d0.getName(), 1);
        put(d1.getName(), 1);
      }
    });

    DomainGroup g2 = coord.addDomainGroup("Group_2");
    g2.addDomain(d1, 0);
    g2.createNewVersion(new HashMap<String, Integer>() {
      {
        put(d1.getName(), 1);
      }
    });

    RingGroup rgAlpha = coord.addRingGroup("RG_Alpha", g1.getName());
    Ring r1 = rgAlpha.addRing(1);
    r1.addHost(addy("alpha-1-1")).addDomain(0).addPartition(0, 1).setCount("Penguins", 4);
    r1.addHost(addy("alpha-1-2"));
    r1.addHost(addy("alpha-1-3"));
    Ring r2 = rgAlpha.addRing(2);
    r2.addHost(addy("alpha-2-1"));
    r2.addHost(addy("alpha-2-2"));
    r2.addHost(addy("alpha-2-3"));
    Ring r3 = rgAlpha.addRing(3);
    r3.addHost(addy("alpha-3-1"));
    r3.addHost(addy("alpha-3-2"));
    r3.addHost(addy("alpha-3-3"));

    RingGroup rgBeta = coord.addRingGroup("RG_Beta", g1.getName());
    r1 = rgBeta.addRing(1);
    r1.addHost(addy("beta-1-1"));
    r1.addHost(addy("beta-1-2"));
    r1.addHost(addy("beta-1-3"));
    r1.addHost(addy("beta-1-4"));
    r2 = rgBeta.addRing(2);
    r2.addHost(addy("beta-2-1"));
    r2.addHost(addy("beta-2-2"));
    r2.addHost(addy("beta-2-3"));
    r2.addHost(addy("beta-2-4"));
    r3 = rgBeta.addRing(3);
    r3.addHost(addy("beta-3-1"));
    r3.addHost(addy("beta-3-2"));
    r3.addHost(addy("beta-3-3"));
    r3.addHost(addy("beta-3-4"));
    Ring r4 = rgBeta.addRing(4);
    r4.addHost(addy("beta-4-1"));
    r4.addHost(addy("beta-4-2"));
    r4.addHost(addy("beta-4-3"));
    r4.addHost(addy("beta-4-4"));

    RingGroup rgGamma = coord.addRingGroup("RG_Gamma", g2.getName());
    r1 = rgGamma.addRing(1);
    r1.addHost(addy("gamma-1-1"));

    ClientConfigurator mockConf = new ClientConfigurator() {
      @Override
      public Coordinator getCoordinator() {
        return coord;
      }
    };
    final Iface mockClient = new Iface() {
      private final Map<String, ByteBuffer> values = new HashMap<String, ByteBuffer>() {
        {
          put("key1", ByteBuffer.wrap("value1".getBytes()));
          put("key2", ByteBuffer.wrap("a really long value that you will just love!".getBytes()));
        }
      };

      @Override
      public HankResponse get(String domainName, ByteBuffer key) throws TException {
        String sKey = null;
        try {
          sKey = new String(key.array(), key.position(), key.limit(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
          throw new TException(e);
        }

        ByteBuffer v = values.get(sKey);
        if (v != null) {
          return HankResponse.value(v);
        }

        return HankResponse.not_found(true);
      }
    };
    IClientCache clientCache = new IClientCache() {
      @Override
      public Iface getSmartClient(RingGroup rgc) throws IOException, TException {
        return mockClient;
      }
    };
    StatusWebDaemon daemon = new StatusWebDaemon(mockConf, clientCache, 12345);
    daemon.run();
  }

  private PartDaemonAddress addy(String hostname) {
    return new PartDaemonAddress(hostname, 6200);
  }
}
