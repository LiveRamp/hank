package com.liveramp.hank.ui;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.thrift.TException;
import org.junit.Ignore;
import org.junit.Test;

import com.liveramp.hank.Hank;
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
import com.liveramp.hank.test.ZkMockCoordinatorTestCase;
import com.liveramp.hank.util.LocalHostUtils;

@Ignore("Should only be run manually")
public class WebUiServerTester extends ZkMockCoordinatorTestCase {

  private static final int NUM_RING_GROUPS = 1;

  public static final String DOMAIN_ = "domain-";
  public static final String DOMAIN_GROUP_ = "domain-group-";
  public static final String RING_GROUP_ = "ring-group-";
  private static final String HOST_ = "host-";
  private static final int NUM_DOMAINS = 2;
  private static final int NUM_RINGS = 2;
  private static final int NUM_HOSTS = 10;
  private static final int NUM_CLIENTS = 100;

  private final Random random = new Random();

  @Test
  public void testIt() throws Exception {
    org.apache.log4j.Logger.getLogger("com.liveramp.hank.zookeeper").setLevel(Level.INFO);

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
        int numRequests = randomNumRequests();
        int numHits = (int)(randomNumHitsRatio() * numRequests);
        int numHitsL1 = (int)(randomL1HitsRatio() * numHits);
        int numHitsL2 = numHits - numHitsL1;
        double requestMinimum = randomRequestMinimum();
        double requestMaximum = randomRequestMaximum();
        long requestValues = randomRequestValues();
        long requestTotal = randomRequestsTotal();
        double[] randomSample = randomRequestSample();
        CacheStatistics randomCacheStatistics = randomCacheStatistics();
        runtimeStatistics.put(hd.getDomain(),
            new RuntimeStatisticsAggregator(
                randomThroughput(),
                randomResponseDataThroughput(),
                numRequests,
                numHits,
                numHitsL1,
                numHitsL2,
                new DoublePopulationStatisticsAggregator(
                    requestMinimum,
                    requestMaximum,
                    requestValues,
                    requestTotal,
                    randomSample),
                randomCacheStatistics
            )
        );
      }
      Hosts.setRuntimeStatistics(host, runtimeStatistics);
    }
    if (state == HostState.SERVING || state == HostState.IDLE) {
      for (HostDomain hd : host.getAssignedDomains()) {
        for (HostDomainPartition partition : hd.getPartitions()) {
          partition.setCurrentDomainVersion(ring.getRingGroup().getDomainGroup().getDomainVersion(hd.getDomain()).getVersionNumber());
        }
      }
    }
    if (state == HostState.UPDATING) {
      Hosts.setUpdateETA(host, randomETA());
      for (HostDomain hd : host.getAssignedDomains()) {
        for (HostDomainPartition partition : hd.getPartitions()) {
          if (random.nextInt(3) == 0) {
            partition.setCurrentDomainVersion(ring.getRingGroup().getDomainGroup().getDomainVersion(hd.getDomain()).getVersionNumber());
          } else {
            if (random.nextInt(3) != 0) {
              partition.setCurrentDomainVersion(ring.getRingGroup().getDomainGroup().getDomainVersion(hd.getDomain()).getVersionNumber() - 1);
            }
          }
        }
      }
    }
    // filesystem
    Map<String, FilesystemStatisticsAggregator> filesystemStatistics = new HashMap<String, FilesystemStatisticsAggregator>();
    for (int i = 0; i < 10; ++i) {
      long totalSpace = randomTotalSpace();
      filesystemStatistics.put("/data-" + i, new FilesystemStatisticsAggregator(totalSpace, randomUsableSpace(totalSpace)));
    }
    Hosts.setFilesystemStatistics(host, filesystemStatistics);
  }

  private void setUpRing(RingGroup ringGroup, int ringGroupId, int ringId) throws IOException {
    Ring ring = ringGroup.addRing(ringId);
    HostState state = randomHostState();
    int numHosts = random.nextInt(NUM_HOSTS) + 10;
    for (int hostId = 0; hostId < numHosts; ++hostId) {
      setUpHost(ringGroupId, ring, hostId, state);
    }
  }

  private Host setUpHost(int ringGroupId, Ring ring, int hostId, HostState state) throws IOException {
    Host host = ring.addHost(addy(HOST_ + ringGroupId + "-" + ring.getRingNumber() + "-" + hostId), Collections.<String>emptyList());
    if (random.nextInt(20) == 0) {
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

  private Domain setUpDomain(int domainGroupId, int domainId, Coordinator coordinator) throws IOException {
    int numPartitions = random.nextInt(32) + 4;
    final Domain domain = coordinator.addDomain(DOMAIN_ + domainGroupId + "-" + domainId, numPartitions, Echo.Factory.class.getName(), "", Murmur64Partitioner.class.getName(), Collections.<String>emptyList());
    DomainVersion ver = domain.openNewVersion(null);
    ver.close();
    return domain;
  }

  private HostState randomHostState() {
    switch (random.nextInt(5)) {
      case 0:
        return HostState.SERVING;
      case 1:
        return HostState.SERVING;
      case 2:
        return HostState.SERVING;
      case 3:
        return HostState.SERVING;
      case 4:
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
    return (random.nextInt(500) + 500) * (long)Math.pow(1021, 3);
  }

  private long randomRequestsTotal() {
    return 10000;
  }

  private long randomRequestValues() {
    return 1000;
  }

  private double randomRequestMaximum() {
    return random.nextInt(10) + 80;
  }

  private double randomRequestMinimum() {
    return random.nextInt(10) * 0.001;
  }

  private double randomL1HitsRatio() {
    return ((double)random.nextInt(100) / 100);
  }

  private double randomNumHitsRatio() {
    return ((double)random.nextInt(25) + 75) / 100;
  }

  private int randomNumRequests() {
    return 142;
  }

  private CacheStatistics randomCacheStatistics() {
    int scale = random.nextInt(10) + 1;
    return new CacheStatistics((1L * scale) << 18, (1L * scale) << 20, (100L * scale) << 20, (1L * scale) << 30);
  }

  private double[] randomRequestSample() {
    int scale = random.nextInt(3) + 1;
    double[] input = new double[]{0.01 * scale, 0.01 * scale, 0.1 * scale, 0.5 * scale, 0.8 * scale, 1 * scale, 1.5 * scale, 3 * scale, 8 * scale};
    double[] result = new double[1000];
    for (int i = 0; i < result.length; ++i) {
      result[i] = input[random.nextInt(input.length)];
    }
    return result;
  }

  private double randomResponseDataThroughput() {
    return random.nextInt(10) * (1 << 20);
  }

  private double randomThroughput() {
    return random.nextInt(1000);
  }
}
