package com.rapleaf.hank.ui;

import junit.framework.TestCase;

import com.rapleaf.hank.config.ClientConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.in_memory.InMemoryCoordinator;

public class StatusWebDaemonTester extends TestCase {
  public void testIt() throws Exception {
//    final DomainConfig domainConfig1 = new MockDomainConfig("Domain 1", 1024, new Murmur64Partitioner(), new ConstantStorageEngine(null), 10);
//    final DomainConfig domainConfig2 = new MockDomainConfig("Domain 2", 1024, new Murmur64Partitioner(), new ConstantStorageEngine(null), 10);
//    final DomainConfig domainConfig3 = new MockDomainConfig("Domain 3", 1024, new Murmur64Partitioner(), new ConstantStorageEngine(null), 10);
//
//    final DomainGroupConfig domainGroup1 = new MockDomainGroupConfig("Domain Group 1") {
//      @Override
//      public SortedSet<DomainGroupConfigVersion> getVersions() {
//        HashSet<DomainConfigVersion> domainVersions1 = new HashSet<DomainConfigVersion>(Arrays.asList(
//            new MockDomainConfigVersion(domainConfig1, 5),
//            new MockDomainConfigVersion(domainConfig3, 7))
//        );
//        HashSet<DomainConfigVersion> domainVersions2 = new HashSet<DomainConfigVersion>(Arrays.asList(
//            new MockDomainConfigVersion(domainConfig1, 6),
//            new MockDomainConfigVersion(domainConfig3, 8))
//        );
//        return new TreeSet<DomainGroupConfigVersion>(Arrays.asList(
//            new MockDomainGroupConfigVersion(domainVersions1, this, 1),
//            new MockDomainGroupConfigVersion(domainVersions2, this, 2)
//        ));
//      }
//    };
//    final DomainGroupConfig domainGroup2 = new MockDomainGroupConfig("Domain Group 2") {
//      @Override
//      public SortedSet<DomainGroupConfigVersion> getVersions() {
//        return new TreeSet<DomainGroupConfigVersion>();
//      }
//    };
//
//    final RingConfig ring1_1 = new MockRingConfig(null, null, 1, RingState.UP) {
//      @Override
//      public Set<HostConfig> getHosts() {
//        return new HashSet<HostConfig>(Arrays.asList(
//            new MockHostConfig(new PartDaemonAddress("h1r1g1.rapleaf.com", 6200)),
//            new MockHostConfig(new PartDaemonAddress("h2r1g1.rapleaf.com", 6200))
//        ));
//      }
//    };
//    final RingConfig ring1_2 = new MockRingConfig(null, null, 2, RingState.UP) {
//      @Override
//      public Set<HostConfig> getHosts() {
//        return new HashSet<HostConfig>(Arrays.asList(
//            new MockHostConfig(new PartDaemonAddress("h1r2g1.rapleaf.com", 6200)),
//            new MockHostConfig(new PartDaemonAddress("h2r2g1.rapleaf.com", 6200))
//        ));
//      }
//    };
//    final RingGroupConfig ringGroup1 = new MockRingGroupConfig(domainGroup1, "Ring Group 1", null) {
//      @Override
//      public Set<RingConfig> getRingConfigs() {
//        return new HashSet<RingConfig>(Arrays.asList(ring1_1, ring1_2));
//      }
//    };
//    final RingGroupConfig ringGroup2 = new MockRingGroupConfig(domainGroup2, "Ring Group 2", null) {
//      @Override
//      public Set<RingConfig> getRingConfigs() {
//        return Collections.EMPTY_SET;
//      }
//    };
//
//    final Coordinator coord = new MockCoordinator() {
//      @Override
//      public Set<DomainConfig> getDomainConfigs() {
//        return new HashSet<DomainConfig>(Arrays.asList(domainConfig1, domainConfig2, domainConfig3));
//      }
//
//      @Override
//      public Set<DomainGroupConfig> getDomainGroupConfigs() {
//        return new HashSet<DomainGroupConfig>(Arrays.asList(domainGroup1, domainGroup2));
//      }
//
//      @Override
//      public Set<RingGroupConfig> getRingGroups() {
//        return new HashSet<RingGroupConfig>(Arrays.asList(ringGroup1, ringGroup2));
//      }
//    };
    final Coordinator coord = new InMemoryCoordinator();
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
