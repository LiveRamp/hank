package com.rapleaf.hank.ui;

import java.util.HashMap;

import junit.framework.TestCase;

import com.rapleaf.hank.config.ClientConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.coordinator.in_memory.InMemoryCoordinator;
import com.rapleaf.hank.partitioner.Murmur64Partitioner;
import com.rapleaf.hank.storage.curly.Curly;

public class StatusWebDaemonTester extends TestCase {
  public void testIt() throws Exception {
    final Coordinator coord = new InMemoryCoordinator();

    final DomainConfig d0 = coord.addDomain("domain0", 1024, Curly.Factory.class.getName(), "---", Murmur64Partitioner.class.getName(), 1);
    final DomainConfig d1 = coord.addDomain("domain1", 1024, Curly.Factory.class.getName(), "---", Murmur64Partitioner.class.getName(), 1);


    DomainGroupConfig g1 = coord.addDomainGroup("Group_1");
    g1.addDomain(d0, 0);
    g1.addDomain(d1, 1);

    g1.createNewVersion(new HashMap<String, Integer>() {{
      put(d0.getName(), 1);
      put(d1.getName(), 1);
    }});

    DomainGroupConfig g2 = coord.addDomainGroup("Group_2");
    g2.addDomain(d1, 0);

    RingGroupConfig rgAlpha = coord.addRingGroup("RG_Alpha", g1.getName());
    RingConfig r1 = rgAlpha.addRing(1);
    RingConfig r2 = rgAlpha.addRing(2);
    RingConfig r3 = rgAlpha.addRing(3);

    RingGroupConfig rgBeta = coord.addRingGroup("RG_Beta", g1.getName());
    r1 = rgBeta.addRing(1);
    r2 = rgBeta.addRing(2);
    r3 = rgBeta.addRing(3);
    RingConfig r4 = rgBeta.addRing(4);

    RingGroupConfig rgGamma = coord.addRingGroup("RG_Gamma", g2.getName());
    r1 = rgGamma.addRing(1);

    ClientConfigurator mockConf = new ClientConfigurator(){
      @Override
      public Coordinator getCoordinator() {
        return coord;
      }
    };
    StatusWebDaemon daemon = new StatusWebDaemon(mockConf, 12345);
    daemon.run();
  }
}
