package com.liveramp.hank.ui;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.thrift.TException;
import org.junit.Test;

import com.liveramp.hank.Hank;
import com.liveramp.hank.ZkMockCoordinatorTestCase;
import com.liveramp.hank.client.HankSmartClient;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostDomainPartition;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.Hosts;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.generated.ClientMetadata;
import com.liveramp.hank.generated.HankBulkResponse;
import com.liveramp.hank.generated.HankResponse;
import com.liveramp.hank.generated.SmartClient;
import com.liveramp.hank.partition_assigner.PartitionAssigner;
import com.liveramp.hank.partition_assigner.RendezVousPartitionAssigner;
import com.liveramp.hank.partition_server.DoublePopulationStatisticsAggregator;
import com.liveramp.hank.partition_server.FilesystemStatisticsAggregator;
import com.liveramp.hank.partition_server.RuntimeStatisticsAggregator;
import com.liveramp.hank.partitioner.Murmur64Partitioner;
import com.liveramp.hank.ring_group_conductor.RingGroupConductorMode;
import com.liveramp.hank.storage.CacheStatistics;
import com.liveramp.hank.storage.echo.Echo;
import com.liveramp.hank.util.LocalHostUtils;

public class WebUiServerTester extends ZkMockCoordinatorTestCase {

  private static final int NUM_RING_GROUPS = 10;

  public static final String DOMAIN_ = "domain-";
  public static final String DOMAIN_GROUP_ = "domain-group-";
  public static final String RING_GROUP_ = "ring-group-";
  private static final String HOST_ = "host-";
  private static final int NUM_DOMAINS = 5;
  private static final int NUM_RINGS = 3;
  private static final int NUM_HOSTS = 20;
  private static final int NUM_CLIENTS = 100;

  private final Random random = new Random();

  @Test
  public void testIt() throws Exception {

    /*
    DomainGroup dg1 = coordinator.getDomainGroup(ZkMockCoordinatorTestCase.DOMAIN_GROUP_0);

    // Assign
    PartitionAssigner partitionAssigner = new RendezVousPartitionAssigner();
    RingGroup rg0 = coordinator.getRingGroup(ZkMockCoordinatorTestCase.RING_GROUP_0);
    RingGroup rg1 = coordinator.getRingGroup(ZkMockCoordinatorTestCase.RING_GROUP_1);
    RingGroup rg2 = coordinator.getRingGroup(ZkMockCoordinatorTestCase.RING_GROUP_2);

    for (Ring ring : rg0.getRings()) {
      partitionAssigner.prepare(ring, dg1.getDomainVersions(), RingGroupConductorMode.ACTIVE);
      for (Host host : ring.getHosts()) {
        partitionAssigner.assign(host);
      }
    }

    // Ring Group 0
    rg0.claimRingGroupConductor(RingGroupConductorMode.INACTIVE);
    for (Ring ring : rg0.getRings()) {
      for (Host host : ring.getHosts()) {
        Map<Domain, RuntimeStatisticsAggregator> runtimeStatistics = new HashMap<Domain, RuntimeStatisticsAggregator>();
        host.setState(HostState.SERVING);
        for (HostDomain hd : host.getAssignedDomains()) {
          runtimeStatistics.put(hd.getDomain(),
              new RuntimeStatisticsAggregator(14, 2500, 142, 100, 15, 48,
                  new DoublePopulationStatisticsAggregator(1.234, 300.1234 * hd.getDomain().getId(), 1000, 10000,
                      new double[]{1, 2, 3, 20, 100, 101, 120, 150, 250}), new CacheStatistics(123L << 20, 1L << 30, 12L << 30, 1L << 40)));
          for (HostDomainPartition partition : hd.getPartitions()) {
            partition.setCurrentDomainVersion(dg1.getDomainVersion(hd.getDomain()).getVersionNumber());
          }
        }
        Hosts.setRuntimeStatistics(host, runtimeStatistics);
        Map<String, FilesystemStatisticsAggregator> filesystemStatistics = new HashMap<String, FilesystemStatisticsAggregator>();
        filesystemStatistics.put("/", new FilesystemStatisticsAggregator(4 * (long)Math.pow(1020, 4), 1 * (long)Math.pow(1023, 4)));
        filesystemStatistics.put("/data", new FilesystemStatisticsAggregator(6 * (long)Math.pow(1021, 4), 3 * (long)Math.pow(1020, 4)));
        Hosts.setFilesystemStatistics(host, filesystemStatistics);
      }
    }

    // Ring Group 1
    // Assign
    for (Ring ring : rg1.getRings()) {
      partitionAssigner.prepare(ring, dg1.getDomainVersions(), RingGroupConductorMode.ACTIVE);
      for (Host host : ring.getHosts()) {
        partitionAssigner.assign(host);
      }
    }
    rg1.claimRingGroupConductor(RingGroupConductorMode.ACTIVE);
    for (Ring ring : rg1.getRings()) {
      // Set first ring to updating
      if (ring.getRingNumber() == rg1.getRings().iterator().next().getRingNumber()) {
        for (Host host : ring.getHosts()) {
          // Set first host to done updating
          if (host.getAddress().equals(ring.getHosts().iterator().next().getAddress())) {
            host.setState(HostState.SERVING);
            for (HostDomain hd : host.getAssignedDomains()) {
              for (HostDomainPartition partition : hd.getPartitions()) {
                partition.setCurrentDomainVersion(dg1.getDomainVersion(hd.getDomain()).getVersionNumber());
              }
            }
          } else {
            // Set other hosts to still updating
            host.setState(HostState.UPDATING);
            // Set fake ETA
            Hosts.setUpdateETA(host, 3243 * ((host.getAddress().hashCode() % 3) + 1));
            for (HostDomain hd : host.getAssignedDomains()) {
              for (HostDomainPartition partition : hd.getPartitions()) {
                partition.setCurrentDomainVersion(0);
              }
            }
          }
        }
      } else {
        for (Host host : ring.getHosts()) {
          host.setState(HostState.SERVING);
          for (HostDomain hd : host.getAssignedDomains()) {
            for (HostDomainPartition partition : hd.getPartitions()) {
              partition.setCurrentDomainVersion(dg1.getDomainVersion(hd.getDomain()).getVersionNumber());
            }
          }
        }
      }
    }

    // Ring Group 2
    for (Ring ring : rg2.getRings()) {
      for (Host host : ring.getHosts()) {

      }
    }

*/

    final SmartClient.Iface mockClient = new SmartClient.Iface() {
      private final Map<String, ByteBuffer> values = new HashMap<String, ByteBuffer>() {
        {
          put("key1", ByteBuffer.wrap("value1".getBytes()));
          put("key2", ByteBuffer.wrap("a really long value that you will just love!".getBytes()));
        }
      };

      @Override
      public HankResponse get(String domainName, ByteBuffer key) throws TException {
        String sKey;
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

      @Override
      public HankBulkResponse getBulk(String domainName, List<ByteBuffer> keys) throws TException {
        return null;
      }
    };
    IClientCache clientCache = new IClientCache() {
      @Override
      public SmartClient.Iface getSmartClient(RingGroup rgc) throws IOException, TException {
        return mockClient;
      }
    };
    WebUiServer uiServer = new WebUiServer(getWebUiMockCoordinator(), clientCache, 12345);
    uiServer.run();
  }

  private Coordinator getWebUiMockCoordinator() throws Exception {
    Coordinator coordinator = getMockCoordinator();
    for (int i = 0; i < NUM_RING_GROUPS; ++i) {
      setUpRingGroup(i, coordinator);
    }
    return coordinator;
  }

  private void setUpRingGroup(int ringGroupId, Coordinator coordinator) throws IOException {
    DomainGroup dg = setUpDomainGroup(ringGroupId, coordinator);
    RingGroup rg = coordinator.addRingGroup(RING_GROUP_ + ringGroupId, dg.getName());
    // Rings
    int numRings = random.nextInt(NUM_RINGS) + 2;
    for (int ringId = 0; ringId < numRings; ++ringId) {
      setUpRing(rg, ringGroupId, ringId);
    }
    // Assign
    PartitionAssigner partitionAssigner = new RendezVousPartitionAssigner();
    for (Ring ring : rg.getRings()) {
      partitionAssigner.prepare(ring, dg.getDomainVersions(), RingGroupConductorMode.ACTIVE);
      for (Host host : ring.getHosts()) {
        partitionAssigner.assign(host);
        updateHost(ring, host);
      }
    }
    rg.claimRingGroupConductor(RingGroupConductorMode.ACTIVE);
    // Clients
    int numClients = random.nextInt(NUM_CLIENTS) + 1;
    for (int clientId = 0; clientId < numClients; ++clientId) {
      rg.registerClient(new ClientMetadata(
          LocalHostUtils.getHostName(),
          System.currentTimeMillis(),
          HankSmartClient.class.getName(),
          Hank.getGitCommit()));
    }
  }

  private void updateHost(Ring ring, Host host) throws IOException {
    HostState state = host.getState();
    // runtime
    if (state == HostState.SERVING) {
      Map<Domain, RuntimeStatisticsAggregator> runtimeStatistics = new HashMap<Domain, RuntimeStatisticsAggregator>();
      for (HostDomain hd : host.getAssignedDomains()) {
        runtimeStatistics.put(hd.getDomain(),
            new RuntimeStatisticsAggregator(randomThroughput(), randomResponseDataThroughput(), 142, 100, 15, 48,
                new DoublePopulationStatisticsAggregator(1.234, 300.1234 * hd.getDomain().getId(), 1000, 10000,
                    new double[]{1, 2, 3, 20, 100, 101, 120, 150, 250}), new CacheStatistics(123L << 20, 1L << 30, 12L << 30, 1L << 40)));
        for (HostDomainPartition partition : hd.getPartitions()) {
          partition.setCurrentDomainVersion(ring.getRingGroup().getDomainGroup().getDomainVersion(hd.getDomain()).getVersionNumber());
        }
      }
      Hosts.setRuntimeStatistics(host, runtimeStatistics);
    }
    if (state == HostState.UPDATING) {
      Hosts.setUpdateETA(host, randomETA());
    }
    // filesystem
    Map<String, FilesystemStatisticsAggregator> filesystemStatistics = new HashMap<String, FilesystemStatisticsAggregator>();
    for (int i = 0; i < 10; ++i) {
      long totalSpace = randomTotalSpace();
      filesystemStatistics.put("/data-" + i, new FilesystemStatisticsAggregator(totalSpace, randomUsableSpace(totalSpace)));
    }
    Hosts.setFilesystemStatistics(host, filesystemStatistics);
  }

  private double randomResponseDataThroughput() {
    return random.nextInt(50) * (2 << 20);
  }

  private double randomThroughput() {
    return random.nextInt(1000);
  }

  private void setUpRing(RingGroup ringGroup, int ringGroupId, int ringId) throws IOException {
    Ring ring = ringGroup.addRing(ringId);
    HostState state = randomHostState();
    int numHosts = random.nextInt(NUM_HOSTS) + 3;
    for (int hostId = 0; hostId < numHosts; ++hostId) {
      setUpHost(ringGroupId, ring, hostId, state);
    }
  }

  private Host setUpHost(int ringGroupId, Ring ring, int hostId, HostState state) throws IOException {
    Host host = ring.addHost(addy(HOST_ + ringGroupId + "-" + ring.getRingNumber() + "-" + hostId), Collections.<String>emptyList());
    if (random.nextInt(10) == 0) {
      state = randomOtherHostState();
    }
    host.setState(state);
    return host;
  }

  private DomainGroup setUpDomainGroup(int domainGroupId, Coordinator coordinator) throws IOException {
    int numDomains = random.nextInt(NUM_DOMAINS) + 3;
    DomainGroup dg = coordinator.addDomainGroup(DOMAIN_GROUP_ + domainGroupId);
    Map<Domain, Integer> versions = new HashMap<Domain, Integer>();
    for (int domainId = 0; domainId < numDomains; ++domainId) {
      Domain domain = setUpDomain(domainGroupId, domainId, coordinator);
      versions.put(domain, 0);
    }
    dg.setDomainVersions(versions);
    return dg;
  }

  private static Domain setUpDomain(int domainGroupId, int domainId, Coordinator coordinator) throws IOException {
    final Domain domain = coordinator.addDomain(DOMAIN_ + domainGroupId + "-" + domainId, 32, Echo.Factory.class.getName(), "", Murmur64Partitioner.class.getName(), Collections.<String>emptyList());
    DomainVersion ver = domain.openNewVersion(null);
    ver.close();
    return domain;
  }

  private HostState randomHostState() {
    switch (random.nextInt(4)) {
      case 0:
        return HostState.SERVING;
      case 1:
        return HostState.SERVING;
      case 2:
        return HostState.SERVING;
      case 3:
        return HostState.UPDATING;
      default:
        throw new IllegalStateException();
    }
  }

  private HostState randomOtherHostState() {
    switch (random.nextInt(2)) {
      case 0:
        return HostState.IDLE;
      case 1:
        return HostState.OFFLINE;
      default:
        throw new IllegalStateException();
    }
  }

  private long randomETA() {
    return random.nextInt(3600);
  }

  private long randomUsableSpace(long totalSpace) {
    return Math.max(0, (totalSpace / (random.nextInt(3) + 3)) - random.nextInt(1 << 30));
  }

  private long randomTotalSpace() {
    return (random.nextInt(5) + 1) * (long)Math.pow(1021, 4);
  }
}
