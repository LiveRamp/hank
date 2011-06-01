/**
 *  Copyright 2011 Rapleaf
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.rapleaf.hank.part_daemon;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.config.PartservConfigurator;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.coordinator.mock.MockCoordinator;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.generated.HankExceptions;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.partitioner.MapPartitioner;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.mock.MockReader;
import com.rapleaf.hank.storage.mock.MockStorageEngine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class TestPartDaemonHandler extends BaseTestCase {
  private static final ByteBuffer K1 = bb(1);
  private static final ByteBuffer K2 = bb(2);
  private static final ByteBuffer K3 = bb(3);
  private static final ByteBuffer K4 = bb(4);
  private static final ByteBuffer K5 = bb(5);
  private static final byte[] V1 = new byte[]{9};
  private static final Host mockHostConfig = new MockHost(new PartDaemonAddress("localhost", 12345)) {

    @Override
    public HostDomain getDomainById(int domainId) {
      return new HostDomain() {
        @Override
        public HostDomainPartition addPartition(int partNum, int initialVersion) {return null;}

        @Override
        public int getDomainId() {
          return 0;
        }

        @Override
        public Set<HostDomainPartition> getPartitions()
        throws IOException {
          return new HashSet<HostDomainPartition>(Arrays.asList(
              new MockHostDomainPartition(0, 1, 2),
              new MockHostDomainPartition(4, 1, 2)));
        }
      };
    }
  };

  public void testSetUpAndServe() throws Exception {
    Partitioner partitioner = new MapPartitioner(K1, 0, K2, 1, K3, 2, K4, 3, K5, 4);
    MockStorageEngine storageEngine = new MockStorageEngine() {
      @Override
      public Reader getReader(PartservConfigurator configurator, int partNum)
      throws IOException {
        return new MockReader(configurator, partNum, V1);
      }
    };
    Domain dc = new MockDomain("myDomain", 5, partitioner, storageEngine, null, null);
    MockDomainGroupVersionDomainVersion dcv = new MockDomainGroupVersionDomainVersion(dc, 1);
    final MockDomainGroupVersion dcgv = new MockDomainGroupVersion(Collections.singleton((DomainGroupVersionDomainVersion)dcv), null, 1);

    final MockDomainGroup dcg = new MockDomainGroup("myDomainGroup") {
      @Override
      public DomainGroupVersion getLatestVersion() {
        return dcgv;
      }

      @Override
      public Integer getDomainId(String domainName) {
        assertEquals("myDomain", domainName);
        return 0;
      }
    };
    final MockRingGroup rgc = new MockRingGroup(dcg, "myRingGroupName", null);

    final MockRing mockRingConfig = new MockRing(null, rgc, 1, RingState.UP) {
      @Override
      public Host getHostByAddress(PartDaemonAddress address) {
        return mockHostConfig;
      }
    };

    Coordinator mockCoordinator = new MockCoordinator() {
      @Override
      public RingGroup getRingGroupConfig(String ringGroupName) {
        assertEquals("myRingGroupName", ringGroupName);
        return new MockRingGroup(dcg, "myRingGroupName", null) {
          @Override
          public Ring getRingForHost(PartDaemonAddress hostAddress) {
            return mockRingConfig;
          }
        };
      }
    };
    PartservConfigurator config = new MockPartDaemonConfigurator(12345, mockCoordinator , "myRingGroupName", "/tmp/local/data/dir");
    PartDaemonHandler handler = new PartDaemonHandler(new PartDaemonAddress("localhost", 12345), config);

    assertEquals(HankResponse.value(V1), handler.get((byte) 0, K1));
    assertEquals(HankResponse.value(V1), handler.get((byte) 0, K5));

    assertEquals(HankResponse.xception(HankExceptions.wrong_host(true)), handler.get((byte) 0, K2));
    assertEquals(HankResponse.xception(HankExceptions.wrong_host(true)), handler.get((byte) 0, K3));
    assertEquals(HankResponse.xception(HankExceptions.wrong_host(true)), handler.get((byte) 0, K4));
  }

  private static ByteBuffer bb(int i) {
    return ByteBuffer.wrap(new byte[]{(byte) i});
  }
}
