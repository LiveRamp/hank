package com.rapleaf.hank.part_daemon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.config.PartDaemonConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainConfigVersion;
import com.rapleaf.hank.coordinator.DomainGroupConfigVersion;
import com.rapleaf.hank.coordinator.MockCoordinator;
import com.rapleaf.hank.coordinator.MockDomainConfig;
import com.rapleaf.hank.coordinator.MockDomainConfigVersion;
import com.rapleaf.hank.coordinator.MockDomainGroupConfig;
import com.rapleaf.hank.coordinator.MockDomainGroupConfigVersion;
import com.rapleaf.hank.coordinator.MockRingConfig;
import com.rapleaf.hank.coordinator.MockRingGroupConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingState;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.partitioner.MapPartitioner;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.MockReader;
import com.rapleaf.hank.storage.MockStorageEngine;
import com.rapleaf.hank.storage.Reader;

public class TestHandler extends BaseTestCase {
  private static final ByteBuffer K1 = bb(1);
  private static final ByteBuffer K2 = bb(2);
  private static final ByteBuffer K3 = bb(3);
  private static final ByteBuffer K4 = bb(4);
  private static final ByteBuffer K5 = bb(5);
  private static final byte[] V1 = new byte[]{9};

  public void testSetUpAndServe() throws Exception {

    Partitioner partitioner = new MapPartitioner(K1, 0, K2, 1, K3, 2, K4, 3, K5, 4);
    MockStorageEngine storageEngine = new MockStorageEngine() {
      @Override
      public Reader getReader(PartDaemonConfigurator configurator, int partNum)
      throws IOException {
        return new MockReader(configurator, partNum, V1);
      }
    };
    DomainConfig dc = new MockDomainConfig("myDomain", 5, partitioner, storageEngine, 1);
    MockDomainConfigVersion dcv = new MockDomainConfigVersion(dc, 1);
    final MockDomainGroupConfigVersion dcgv = new MockDomainGroupConfigVersion(Collections.singleton((DomainConfigVersion)dcv), null, 1);

    final MockDomainGroupConfig dcg = new MockDomainGroupConfig("myDomainGroup") {
      @Override
      public DomainGroupConfigVersion getLatestVersion() {
        return dcgv;
      }

      @Override
      public int getDomainId(String domainName) throws DataNotFoundException {
        assertEquals("myDomain", domainName);
        return 0;
      }
    };
    final MockRingGroupConfig rgc = new MockRingGroupConfig(dcg, "myRingGroupName", null);

    final MockRingConfig mockRingConfig = new MockRingConfig(null, rgc, 1, RingState.AVAILABLE) {
      @Override
      public Set<Integer> getDomainPartitionsForHost(PartDaemonAddress hostAndPort, int domainId)
      throws DataNotFoundException {
        assertEquals(new PartDaemonAddress("localhost", 12345), hostAndPort);
        assertEquals(0, domainId);
        return new HashSet<Integer>(Arrays.asList(0, 4));
      }
    };

    Coordinator mockCoordinator = new MockCoordinator() {
      @Override
      public RingConfig getRingConfig(String ringGroupName, int ringNumber)
      throws DataNotFoundException {
        assertEquals("myRingGroupName", ringGroupName);
        assertEquals(1, ringNumber);
        return mockRingConfig;
      }
    };
    PartDaemonConfigurator config = new MockPartDaemonConfigurator(12345, mockCoordinator , "myRingGroupName", 1, "/tmp/local/data/dir");
    Handler handler = new Handler(new PartDaemonAddress("localhost", 12345), config);

    assertEquals(HankResponse.value(V1), handler.get((byte) 0, K1));
    assertEquals(HankResponse.value(V1), handler.get((byte) 0, K5));

    assertEquals(HankResponse.wrong_host(true), handler.get((byte) 0, K2));
    assertEquals(HankResponse.wrong_host(true), handler.get((byte) 0, K3));
    assertEquals(HankResponse.wrong_host(true), handler.get((byte) 0, K4));
  }

  private static ByteBuffer bb(int i) {
    return ByteBuffer.wrap(new byte[]{(byte) i});
  }
}
