package com.liveramp.hank.ui;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.junit.Test;

import com.liveramp.hank.ZkMockCoordinatorTestCase;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostDomainPartition;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.Hosts;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.generated.HankBulkResponse;
import com.liveramp.hank.generated.HankResponse;
import com.liveramp.hank.generated.SmartClient.Iface;
import com.liveramp.hank.partition_assigner.PartitionAssigner;
import com.liveramp.hank.partition_assigner.RendezVousPartitionAssigner;
import com.liveramp.hank.partition_server.DoublePopulationStatisticsAggregator;
import com.liveramp.hank.partition_server.FilesystemStatisticsAggregator;
import com.liveramp.hank.partition_server.RuntimeStatisticsAggregator;
import com.liveramp.hank.ring_group_conductor.RingGroupConductorMode;
import com.liveramp.hank.storage.CacheStatistics;

public class WebUiServerTester extends ZkMockCoordinatorTestCase {

  @Test
  public void testIt() throws Exception {
    final Coordinator coordinator = getApiMockCoordinator();

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
        host.setState(HostState.IDLE);
      }
    }

    final Iface mockClient = new Iface() {
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
      public Iface getSmartClient(RingGroup rgc) throws IOException, TException {
        return mockClient;
      }
    };
    WebUiServer uiServer = new WebUiServer(coordinator, clientCache, 12345);
    uiServer.run();
  }
}
