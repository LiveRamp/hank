package com.rapleaf.hank.ui;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.SmartClient.Iface;
import com.rapleaf.hank.partition_assigner.PartitionAssigner;
import com.rapleaf.hank.partition_assigner.UniformPartitionAssigner;
import org.apache.thrift.TException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WebUiServerTester extends ZkTestCase {
  public void testIt() throws Exception {
    final Coordinator coordinator = getMockCoordinator();

    DomainGroup dg1 = coordinator.getDomainGroup("Group_1");
    DomainGroup dg2 = coordinator.getDomainGroup("Group_2");

    // Assign
    PartitionAssigner partitionAssigner = new UniformPartitionAssigner();
    RingGroup rgAlpha = coordinator.getRingGroup("RG_Alpha");
    RingGroup rgBeta = coordinator.getRingGroup("RG_Beta");
    RingGroup rgGamma = coordinator.getRingGroup("RG_Gamma");

    DomainGroupVersion dgv = DomainGroups.getLatestVersion(dg1);
    for (Ring ring : rgAlpha.getRings()) {
      partitionAssigner.assign(dgv, ring);
    }

    // Ring ALPHA
    rgAlpha.setUpdatingToVersion(dgv.getVersionNumber());
    rgAlpha.updateComplete();
    for (Ring ring : rgAlpha.getRings()) {
      ring.setState(RingState.OPEN);
      ring.setUpdatingToVersion(dgv.getVersionNumber());
      ring.updateComplete();
      for (Host host : ring.getHosts()) {
        host.setState(HostState.SERVING);
        for (HostDomain hd : host.getAssignedDomains()) {
          for (HostDomainPartition partition : hd.getPartitions()) {
            partition.setUpdatingToDomainGroupVersion(null);
            partition.setCurrentDomainGroupVersion(dgv.getVersionNumber());
          }
        }
      }
    }

    // Ring BETA
    rgBeta.setUpdatingToVersion(0);
    rgBeta.updateComplete();
    // Assign
    for (Ring ring : rgBeta.getRings()) {
      partitionAssigner.assign(dgv, ring);
    }
    rgBeta.setUpdatingToVersion(1);
    for (Ring ring : rgBeta.getRings()) {
      // Set first ring to updating
      if (ring.getRingNumber() == rgBeta.getRings().iterator().next().getRingNumber()) {
        ring.setState(RingState.UPDATING);
        ring.setUpdatingToVersion(0);
        ring.updateComplete();
        ring.setUpdatingToVersion(dgv.getVersionNumber());
        for (Host host : ring.getHosts()) {
          // Set first host to done updating
          if (host.getAddress().equals(ring.getHosts().iterator().next().getAddress())) {
            host.setState(HostState.SERVING);
            for (HostDomain hd : host.getAssignedDomains()) {
              for (HostDomainPartition partition : hd.getPartitions()) {
                partition.setCurrentDomainGroupVersion(dgv.getVersionNumber());
                partition.setUpdatingToDomainGroupVersion(null);
              }
            }
          } else {
            // Set other hosts to still updating
            host.setState(HostState.UPDATING);
            for (HostDomain hd : host.getAssignedDomains()) {
              for (HostDomainPartition partition : hd.getPartitions()) {
                partition.setCurrentDomainGroupVersion(0);
                partition.setUpdatingToDomainGroupVersion(dgv.getVersionNumber());
              }
            }
          }
        }
      } else {
        ring.setUpdatingToVersion(dgv.getVersionNumber());
        ring.updateComplete();
        ring.setState(RingState.OPEN);
        for (Host host : ring.getHosts()) {
          host.setState(HostState.SERVING);
          for (HostDomain hd : host.getAssignedDomains()) {
            for (HostDomainPartition partition : hd.getPartitions()) {
              partition.setCurrentDomainGroupVersion(dgv.getVersionNumber());
              partition.setUpdatingToDomainGroupVersion(null);
            }
          }
        }
      }
    }

    // Ring GAMMA
    rgGamma.updateComplete();
    for (Ring ring : rgGamma.getRings()) {
      ring.setState(RingState.OPEN);
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
