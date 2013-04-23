package com.liveramp.hank.ui;

import com.liveramp.hank.ZkMockCoordinatorTestCase;
import com.liveramp.hank.coordinator.*;
import com.liveramp.hank.generated.HankBulkResponse;
import com.liveramp.hank.generated.HankResponse;
import com.liveramp.hank.generated.SmartClient.Iface;
import com.liveramp.hank.partition_assigner.ModPartitionAssigner;
import com.liveramp.hank.partition_assigner.PartitionAssigner;
import com.liveramp.hank.partition_server.DoublePopulationStatisticsAggregator;
import com.liveramp.hank.partition_server.FilesystemStatisticsAggregator;
import com.liveramp.hank.partition_server.RuntimeStatisticsAggregator;
import com.liveramp.hank.ring_group_conductor.RingGroupConductorMode;
import org.apache.thrift.TException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WebUiServerTester extends ZkMockCoordinatorTestCase {

  public void testIt() throws Exception {
    final Coordinator coordinator = getMockCoordinator();

    DomainGroup dg1 = coordinator.getDomainGroup("Group_1");

    // Assign
    PartitionAssigner partitionAssigner = new ModPartitionAssigner();
    RingGroup rgAlpha = coordinator.getRingGroup("RG_Alpha");
    RingGroup rgBeta = coordinator.getRingGroup("RG_Beta");
    RingGroup rgGamma = coordinator.getRingGroup("RG_Gamma");

    for (Ring ring : rgAlpha.getRings()) {
      partitionAssigner.assign(ring, dg1);
    }

    // Ring ALPHA
    rgAlpha.claimRingGroupConductor(RingGroupConductorMode.INACTIVE);
    for (Ring ring : rgAlpha.getRings()) {
      for (Host host : ring.getHosts()) {
        Map<Domain, RuntimeStatisticsAggregator> runtimeStatistics = new HashMap<Domain, RuntimeStatisticsAggregator>();
        host.setState(HostState.SERVING);
        for (HostDomain hd : host.getAssignedDomains()) {
          runtimeStatistics.put(hd.getDomain(),
              new RuntimeStatisticsAggregator(14, 2500, 142, 100, 15, 48,
                  new DoublePopulationStatisticsAggregator(1.234, 300.1234 * hd.getDomain().getId(), 1000, 10000,
                      new double[]{1, 2, 3, 20, 100, 101, 120, 150, 250})));
          for (HostDomainPartition partition : hd.getPartitions()) {
            partition.setCurrentDomainVersion(dg1.getDomainVersion(hd.getDomain()).getVersionNumber());
          }
        }
        Hosts.setRuntimeStatistics(host, runtimeStatistics);
        Map<String, FilesystemStatisticsAggregator> filesystemStatistics = new HashMap<String, FilesystemStatisticsAggregator>();
        filesystemStatistics.put("/", new FilesystemStatisticsAggregator(4 * (long) Math.pow(1020, 4), 1 * (long) Math.pow(1023, 4)));
        filesystemStatistics.put("/data", new FilesystemStatisticsAggregator(6 * (long) Math.pow(1021, 4), 3 * (long) Math.pow(1020, 4)));
        Hosts.setFilesystemStatistics(host, filesystemStatistics);
      }
    }

    // Ring BETA
    // Assign
    for (Ring ring : rgBeta.getRings()) {
      partitionAssigner.assign(ring, dg1);
    }
    rgBeta.claimRingGroupConductor(RingGroupConductorMode.ACTIVE);
    for (Ring ring : rgBeta.getRings()) {
      // Set first ring to updating
      if (ring.getRingNumber() == rgBeta.getRings().iterator().next().getRingNumber()) {
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

    // Ring GAMMA
    for (Ring ring : rgGamma.getRings()) {
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
