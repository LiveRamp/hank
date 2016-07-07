/**
 *  Copyright 2011 LiveRamp
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
package com.liveramp.hank.partition_server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.junit.Test;

import com.liveramp.hank.config.PartitionServerConfigurator;
import com.liveramp.hank.config.ReaderConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainAndVersion;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostDomainPartition;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.coordinator.mock.MockCoordinator;
import com.liveramp.hank.coordinator.mock.MockDomain;
import com.liveramp.hank.coordinator.mock.MockDomainGroup;
import com.liveramp.hank.generated.HankBulkResponse;
import com.liveramp.hank.generated.HankException;
import com.liveramp.hank.generated.HankResponse;
import com.liveramp.hank.partitioner.Partitioner;
import com.liveramp.hank.storage.Reader;
import com.liveramp.hank.storage.mock.MockReader;
import com.liveramp.hank.storage.mock.MockStorageEngine;
import com.liveramp.hank.test.BaseTestCase;
import com.liveramp.hank.test.coordinator.MockHost;
import com.liveramp.hank.test.coordinator.MockHostDomain;
import com.liveramp.hank.test.coordinator.MockHostDomainPartition;
import com.liveramp.hank.test.coordinator.MockRing;
import com.liveramp.hank.test.coordinator.MockRingGroup;
import com.liveramp.hank.test.partitioner.MapPartitioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestPartitionServerHandler extends BaseTestCase {

  private static final ByteBuffer K1 = bb(1);
  private static final ByteBuffer K2 = bb(2);
  private static final ByteBuffer K3 = bb(3);
  private static final ByteBuffer K4 = bb(4);
  private static final ByteBuffer K5 = bb(5);
  private static final byte[] V1 = new byte[]{9};
  private static final Host mockHostConfig = new MockHost(
      new PartitionServerAddress("localhost", 12345)) {

    @Override
    public HostDomain getHostDomain(Domain domain) {

      return new MockHostDomain(domain) {
        @Override
        public HostDomainPartition addPartition(int partitionNumber) {
          return null;
        }

        @Override
        public Set<HostDomainPartition> getPartitions() throws IOException {
          return new HashSet<HostDomainPartition>(Arrays.asList(
              new MockHostDomainPartition(0, 0),
              new MockHostDomainPartition(4, 0)));
        }
      };
    }
  };

  @Test
  public void testDontServeNotUpToDatePartition() throws IOException, TException {
    try {
      PartitionServerHandler handler = createHandler(42);
      fail("Should throw an exception.");
    } catch (IOException e) {
    }
  }

  @Test
  public void testSetUpAndServe() throws Exception {
    PartitionServerHandler handler = createHandler(0);

    assertEquals(HankResponse.value(V1), handler.get((byte)0, K1));
    assertEquals(HankResponse.value(V1), handler.get((byte)0, K5));

    assertEquals(HankResponse.xception(HankException.wrong_host(true)),
        handler.get(0, K2));
    assertEquals(HankResponse.xception(HankException.wrong_host(true)),
        handler.get(0, K3));
    assertEquals(HankResponse.xception(HankException.wrong_host(true)),
        handler.get(0, K4));
  }

  @Test
  public void testSetUpAndServeBulk() throws Exception {
    PartitionServerHandler handler = createHandler(0);

    // Regular bulk request
    List<ByteBuffer> keys1 = new ArrayList<ByteBuffer>();
    keys1.add(K1);
    keys1.add(K2);
    keys1.add(K5);

    ArrayList<HankResponse> responses1 = new ArrayList<HankResponse>();
    responses1.add(HankResponse.value(V1));
    responses1.add(HankResponse.xception(HankException.wrong_host(true)));
    responses1.add(HankResponse.value(V1));

    assertEquals(HankBulkResponse.responses(responses1), handler.getBulk(0, keys1));

    // Large bulk request
    List<ByteBuffer> keys2 = new ArrayList<ByteBuffer>();
    ArrayList<HankResponse> responses2 = new ArrayList<HankResponse>();
    for (int i = 0; i < 10000; ++i) {
      keys2.add(K1);
      responses2.add(HankResponse.value(V1));
    }
    assertEquals(HankBulkResponse.responses(responses2), handler.getBulk(0, keys2));
  }

  private PartitionServerHandler createHandler(final int readerVersionNumber) throws IOException {
    Partitioner partitioner = new MapPartitioner(K1, 0, K2, 1, K3, 2, K4, 3,
        K5, 4);
    MockStorageEngine storageEngine = new MockStorageEngine() {
      @Override
      public Reader getReader(ReaderConfigurator configurator, int partitionNumber, DiskPartitionAssignment assignment)
          throws IOException {
        return new MockReader(configurator, partitionNumber, V1, readerVersionNumber) {
          @Override
          public Integer getVersionNumber() {
            return readerVersionNumber;
          }
        };
      }
    };
    final Domain domain = new MockDomain("myDomain", 0, 5, partitioner, storageEngine, null, null);

    final MockDomainGroup dg = new MockDomainGroup("myDomainGroup") {
      @Override
      public Set<DomainAndVersion> getDomainVersions() throws IOException {
        Set<DomainAndVersion> result = new HashSet<DomainAndVersion>();
        result.add(new DomainAndVersion(domain, 1));
        return result;
      }
    };
    final MockRingGroup rg = new MockRingGroup(dg, "myRingGroupName", null);

    final MockRing mockRing = new MockRing(null, rg, 1) {
      @Override
      public Host getHostByAddress(PartitionServerAddress address) {
        return mockHostConfig;
      }
    };

    Coordinator mockCoordinator = new MockCoordinator() {
      @Override
      public RingGroup getRingGroup(String ringGroupName) {
        assertEquals("myRingGroupName", ringGroupName);
        return new MockRingGroup(dg, "myRingGroupName", null) {
          @Override
          public Ring getRingForHost(PartitionServerAddress hostAddress) {
            return mockRing;
          }
        };
      }
    };
    PartitionServerConfigurator config = new MockPartitionServerConfigurator(12345,
        mockCoordinator, "myRingGroupName", "/tmp/local/data/dir");
    PartitionServerHandler handler = new PartitionServerHandler(new PartitionServerAddress(
        "localhost", 12345), config, mockCoordinator);
    return handler;
  }

  private static ByteBuffer bb(int i) {
    return ByteBuffer.wrap(new byte[]{(byte)i});
  }
}
