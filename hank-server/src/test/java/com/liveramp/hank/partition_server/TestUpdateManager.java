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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainAndVersion;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostDomainPartition;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.coordinator.mock.MockDomain;
import com.liveramp.hank.coordinator.mock.MockDomainGroup;
import com.liveramp.hank.coordinator.mock.MockDomainVersion;
import com.liveramp.hank.partitioner.ConstantPartitioner;
import com.liveramp.hank.storage.Deleter;
import com.liveramp.hank.storage.PartitionUpdater;
import com.liveramp.hank.storage.StorageEngine;
import com.liveramp.hank.storage.mock.MockDeleter;
import com.liveramp.hank.storage.mock.MockStorageEngine;
import com.liveramp.hank.test.BaseTestCase;
import com.liveramp.hank.test.coordinator.MockHost;
import com.liveramp.hank.test.coordinator.MockHostDomain;
import com.liveramp.hank.test.coordinator.MockHostDomainPartition;
import com.liveramp.hank.test.coordinator.MockRing;
import com.liveramp.hank.test.coordinator.MockRingGroup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestUpdateManager extends BaseTestCase {

  private Fixtures fixtures;

  @Before
  public void setUp() throws Exception {
    this.fixtures = new Fixtures();
  }

  private static class Fixtures {

    public class MockRingGroupLocal extends MockRingGroup {

      private Ring ringSingleton = null;

      private MockRingGroupLocal(DomainGroup dcg, String name) {
        super(dcg, name, Collections.<Ring>emptySet());
      }

      @Override
      public Ring getRingForHost(PartitionServerAddress hostAddress) {
        return ringSingleton;
      }

      public void setRing(Ring ring) {
        this.ringSingleton = ring;
      }
    }

    private MockRingGroupLocal getMockRingGroup(DomainGroup domainGroup) {
      return new MockRingGroupLocal(domainGroup, "myRingGroup") {
      };
    }

    private Ring getMockRing(final Host host, final RingGroup ringGroup) {
      return new MockRing(null, ringGroup, 0) {
        @Override
        public Host getHostByAddress(PartitionServerAddress address) {
          return host;
        }
      };
    }

    private final MockDeleter MOCK_DELETER = new MockDeleter(1);

    protected final class MSE extends MockStorageEngine {
      private final PartitionUpdater updater;

      private MSE(PartitionUpdater updater) {
        this.updater = updater;
      }

      @Override
      public PartitionUpdater getUpdater(DiskPartitionAssignment assignment, int partitionNumber) {
        return updater;
      }

      @Override
      public Deleter getDeleter(DiskPartitionAssignment assignment, int partitionNumber)
          throws IOException {
        return MOCK_DELETER;
      }
    }

    protected StorageEngine getMockStorageEngine(PartitionUpdater updater) {
      return new MSE(updater);
    }

    private final MockHostDomainPartition HOST_DOMAIN_PARTITION = new MockHostDomainPartition(0, 0);
    private final MockHostDomainPartition PARTITION_FOR_DELETION = new MockHostDomainPartition(1, 0);

    public class MockHostDomainLocal extends MockHostDomain {

      private boolean emptyMode = false;

      public MockHostDomainLocal(Domain domain) {
        super(domain);
      }

      @Override
      public Set<HostDomainPartition> getPartitions() throws IOException {
        Set<HostDomainPartition> partitions = new HashSet<HostDomainPartition>();
        if (!emptyMode) {
          partitions.add(HOST_DOMAIN_PARTITION);
          partitions.add(PARTITION_FOR_DELETION);
        }
        return partitions;
      }

      @Override
      public HostDomainPartition addPartition(int partitionNumber) {
        return null;
      }

      public void setEmptyMode(boolean emptyMode) {
        this.emptyMode = emptyMode;
      }
    }

    private MockHostDomainLocal getMockHostDomain(Domain domain) {
      return new MockHostDomainLocal(domain);
    }

    private MockHost getMockHost(final HostDomain hostDomain) {
      return new MockHost(new PartitionServerAddress("localhost", 1)) {
        @Override
        public HostDomain getHostDomain(Domain domain) {
          return hostDomain;
        }

        @Override
        public Set<HostDomain> getAssignedDomains() {
          return Collections.singleton(hostDomain);
        }
      };
    }

    private Domain getMockDomain(final StorageEngine storageEngine) {
      return new MockDomain("myDomain", 1, 1,
          new ConstantPartitioner(), storageEngine, null, null) {
        @Override
        public StorageEngine getStorageEngine() {
          return storageEngine;
        }

        @Override
        public SortedSet<DomainVersion> getVersions() {
          return new TreeSet<DomainVersion>(
              Arrays.asList(new MockDomainVersion(2, null) {
                @Override
                public boolean isDefunct() throws IOException {
                  return true;
                }
              }));
        }
      };
    }

    private DomainGroup getMockDomainGroup(final Domain domain) {
      // the domain version for this domain group version will be 2
      return new MockDomainGroup("myDomainGroup") {
        @Override
        public Set<DomainAndVersion> getDomainVersions() {
          Set<DomainAndVersion> result = new HashSet<DomainAndVersion>();
          result.add(new DomainAndVersion(domain, 2));
          return result;
        }
      };
    }
  }

  @Test
  public void testUpdate() throws Exception {
    final MockPartitionUpdater mockUpdater = new MockPartitionUpdater();

    StorageEngine mockStorageEngine = fixtures.getMockStorageEngine(mockUpdater);
    Domain mockDomain = fixtures.getMockDomain(mockStorageEngine);
    MockHostDomain mockHostDomain = fixtures.getMockHostDomain(mockDomain);
    Host mockHost = fixtures.getMockHost(mockHostDomain);
    DomainGroup mockDomainGroup = fixtures.getMockDomainGroup(mockDomain);
    Fixtures.MockRingGroupLocal mockRingGroup = fixtures.getMockRingGroup(mockDomainGroup);
    Ring mockRing = fixtures.getMockRing(mockHost, mockRingGroup);
    mockRingGroup.setRing(mockRing);

    UpdateManager ud = new UpdateManager(new MockPartitionServerConfigurator(1,
        null, "myRingGroup", "/local/data/dir"), mockHost,
        mockRingGroup);
    ud.update();
    assertTrue("update() was called on the storage engine",
        mockUpdater.isUpdated());
    assertEquals("update() called with proper args", Integer.valueOf(2),
        mockUpdater.updatedToVersion);
    assertEquals("current version", Integer.valueOf(2),
        fixtures.HOST_DOMAIN_PARTITION.getCurrentDomainVersion());

    assertFalse("host domain contains the partition", fixtures.MOCK_DELETER.hasDeleted());
    assertFalse("host domain partition has not yet been deleted",
        mockHostDomain.isRemoved(fixtures.PARTITION_FOR_DELETION.getPartitionNumber()));

    // Test partition deletion
    fixtures.PARTITION_FOR_DELETION.setDeletable(true);
    ud.update();

    assertTrue("host domain does not contain the partition",
        fixtures.MOCK_DELETER.hasDeleted());
    assertTrue("host domain partition has been deleted",
        mockHostDomain.isRemoved(fixtures.PARTITION_FOR_DELETION.getPartitionNumber()));
  }

  @Test
  public void testGarbageCollectDomain() throws Exception {
    final MockPartitionUpdater mockUpdater = new MockPartitionUpdater();

    StorageEngine mockStorageEngine = fixtures.getMockStorageEngine(mockUpdater);
    Domain mockDomain = fixtures.getMockDomain(mockStorageEngine);
    Fixtures.MockHostDomainLocal mockHostDomain = fixtures.getMockHostDomain(mockDomain);
    MockHost mockHost = fixtures.getMockHost(mockHostDomain);
    // Empty domain group version
    DomainGroup mockDomainGroup = new MockDomainGroup("myDomainGroup") {
      @Override
      public Set<DomainAndVersion> getDomainVersions() {
        return Collections.emptySet();
      }
    };
    Fixtures.MockRingGroupLocal mockRingGroup = fixtures.getMockRingGroup(mockDomainGroup);
    Ring mockRing = fixtures.getMockRing(mockHost, mockRingGroup);
    mockRingGroup.setRing(mockRing);

    UpdateManager ud = new UpdateManager(new MockPartitionServerConfigurator(1,
        null, "myRingGroup", "/local/data/dir"), mockHost,
        mockRingGroup);

    ud.update();

    assertFalse("update() was not called on the storage engine",
        mockUpdater.isUpdated());

    assertTrue("the partition was deleted", fixtures.MOCK_DELETER.hasDeleted());
    assertTrue("host domain partition has been deleted", mockHostDomain.isRemoved(fixtures.PARTITION_FOR_DELETION.getPartitionNumber()));
    assertFalse("host domain has not been deleted", mockHost.isRemoved(mockHostDomain.getDomain()));

    mockHostDomain.setEmptyMode(true);

    ud.update();

    assertTrue("host domain has been deleted", mockHost.isRemoved(mockHostDomain.getDomain()));
  }

  @Test
  public void testFailedUpdateTask() throws Exception {
    final MockPartitionUpdater failingUpdater = new MockPartitionUpdater() {
      @Override
      public void updateTo(DomainVersion updatingToVersion, PartitionUpdateTaskStatistics statistics) throws IOException {
        super.updateTo(updatingToVersion, statistics);
        throw new IOException("Failed to update.");
      }
    };

    StorageEngine mockStorageEngine = fixtures.getMockStorageEngine(failingUpdater);
    Domain mockDomain = fixtures.getMockDomain(mockStorageEngine);
    HostDomain mockHostDomain = fixtures.getMockHostDomain(mockDomain);
    Host mockHost = fixtures.getMockHost(mockHostDomain);
    DomainGroup mockDomainGroup = fixtures.getMockDomainGroup(mockDomain);
    Fixtures.MockRingGroupLocal mockRingGroup = fixtures.getMockRingGroup(mockDomainGroup);
    Ring mockRing = fixtures.getMockRing(mockHost, mockRingGroup);
    mockRingGroup.setRing(mockRing);

    UpdateManager ud = new UpdateManager(new MockPartitionServerConfigurator(1,
        null, "myRingGroup", "/local/data/dir"), mockHost,
        mockRingGroup);

    try {
      ud.update();
      assertTrue("update() should have been called on the storage engine", failingUpdater.isUpdated());
      fail("Should throw an IOException when a task update fails.");
    } catch (IOException e) {
      // Correct behavior
    }
    // All updates have failed, so all versions should be set to null
    for (HostDomainPartition partition : mockHostDomain.getPartitions()) {
      assertEquals(null, partition.getCurrentDomainVersion());
    }
  }

  @Test
  public void testInterruptedUpdateTask() throws Exception {
    final MockPartitionUpdater mockUpdater = new MockPartitionUpdater();

    StorageEngine mockStorageEngine = fixtures.getMockStorageEngine(mockUpdater);
    Domain mockDomain = fixtures.getMockDomain(mockStorageEngine);
    HostDomain mockHostDomain = fixtures.getMockHostDomain(mockDomain);
    Host mockHost = fixtures.getMockHost(mockHostDomain);
    DomainGroup mockDomainGroup = fixtures.getMockDomainGroup(mockDomain);
    Fixtures.MockRingGroupLocal mockRingGroup = fixtures.getMockRingGroup(mockDomainGroup);
    Ring mockRing = fixtures.getMockRing(mockHost, mockRingGroup);
    mockRingGroup.setRing(mockRing);

    UpdateManager ud = new UpdateManager(new MockPartitionServerConfigurator(1,
        null, "myRingGroup", "/local/data/dir"), mockHost,
        mockRingGroup);

    try {
      // Interrupt to simulate update cancellation
      Thread.currentThread().interrupt();
      // Launch update
      ud.update();
      assertTrue("update() should have been called on the storage engine", mockUpdater.isUpdated());
      fail("Should throw an IOException when update is interrupted.");
    } catch (IOException e) {
      // Correct behavior
    }
  }
}
