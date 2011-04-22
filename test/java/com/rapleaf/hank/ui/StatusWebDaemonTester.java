package com.rapleaf.hank.ui;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import com.rapleaf.hank.config.ClientConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.MockCoordinator;
import com.rapleaf.hank.coordinator.MockDomainConfig;
import com.rapleaf.hank.coordinator.MockDomainGroupConfig;
import com.rapleaf.hank.coordinator.MockRingGroupConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.partitioner.Murmur64Partitioner;
import com.rapleaf.hank.storage.constant.ConstantStorageEngine;

public class StatusWebDaemonTester extends TestCase {
  public void testIt() throws Exception {
    final DomainConfig domainConfig1 = new MockDomainConfig("Domain 1", 1024, new Murmur64Partitioner(), new ConstantStorageEngine(null), 10);
    final DomainConfig domainConfig2 = new MockDomainConfig("Domain 2", 1024, new Murmur64Partitioner(), new ConstantStorageEngine(null), 10);
    final DomainConfig domainConfig3 = new MockDomainConfig("Domain 3", 1024, new Murmur64Partitioner(), new ConstantStorageEngine(null), 10);

    final DomainGroupConfig domainGroup1 = new MockDomainGroupConfig("Domain Group 1");
    final DomainGroupConfig domainGroup2 = new MockDomainGroupConfig("Domain Group 2");

    final RingGroupConfig ringGroup1 = new MockRingGroupConfig(domainGroup1, "Ring Group 1", null);
    final RingGroupConfig ringGroup2 = new MockRingGroupConfig(domainGroup2, "Ring Group 2", null);

    final Coordinator coord = new MockCoordinator() {
      @Override
      public Set<DomainConfig> getDomainConfigs() {
        return new HashSet<DomainConfig>(Arrays.asList(domainConfig1, domainConfig2, domainConfig3));
      }

      @Override
      public Set<DomainGroupConfig> getDomainGroupConfigs() {
        return new HashSet<DomainGroupConfig>(Arrays.asList(domainGroup1, domainGroup2));
      }

      @Override
      public Set<RingGroupConfig> getRingGroups() {
        return new HashSet<RingGroupConfig>(Arrays.asList(ringGroup1, ringGroup2));
      }
    };
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
