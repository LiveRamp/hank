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
package com.rapleaf.hank.partition_server;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.config.PartitionServerConfigurator;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainGroup;
import com.rapleaf.hank.partitioner.ConstantPartitioner;
import com.rapleaf.hank.storage.Deleter;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.Updater;
import com.rapleaf.hank.storage.mock.MockDeleter;
import com.rapleaf.hank.storage.mock.MockStorageEngine;

import java.io.IOException;
import java.util.*;

public class TestUpdateManager extends BaseTestCase {

  private Fixtures fixtures;

  public void setUp() throws Exception {
    super.setUp();
    this.fixtures = new Fixtures();
  }

  private static class Fixtures {

    private class MRG extends MockRingGroup {
      private MRG(DomainGroup dcg, String name, Set<Ring> rings) {
        super(dcg, name, rings);
      }
    }

    private RingGroup getMockRingGroup(DomainGroup domainGroup, final Ring ring) {
      return new MRG(domainGroup, "myRingGroup", null) {
        @Override
        public Integer getUpdatingToVersion() {
          try {
            return DomainGroups.getLatestVersion(getDomainGroup()).getVersionNumber();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public Ring getRingForHost(PartitionServerAddress hostAddress) {
          return ring;
        }
      };
    }

    private Ring getMockRing(final Host host) {
      return new MockRing(null, null, 0, null) {
        @Override
        public Host getHostByAddress(PartitionServerAddress address) {
          return host;
        }
      };
    }

    private final MockDeleter MOCK_DELETER = new MockDeleter(1);

    protected final class MSE extends MockStorageEngine {
      private final Updater updater;

      private MSE(Updater updater) {
        this.updater = updater;
      }

      @Override
      public Updater getUpdater(PartitionServerConfigurator configurator, int partNum) {
        return updater;
      }

      @Override
      public Deleter getDeleter(PartitionServerConfigurator configurator, int partNum)
          throws IOException {
        return MOCK_DELETER;
      }
    }

    protected StorageEngine getMockStorageEngine(Updater updater) {
      return new MSE(updater);
    }

    private final HostDomainPartition HOST_DOMAIN_PARTITION = new AbstractHostDomainPartition() {
      private Integer updatingToVersion = 1;
      private Integer currentVersion = 0;
      private boolean deletable = false;

      @Override
      public void setUpdatingToDomainGroupVersion(Integer version)
          throws IOException {
        updatingToVersion = version;
      }

      @Override
      public void setCurrentDomainGroupVersion(int version) throws IOException {
        currentVersion = version;
      }

      @Override
      public Integer getUpdatingToDomainGroupVersion() throws IOException {
        return updatingToVersion;
      }

      @Override
      public int getPartitionNumber() {
        return 0;
      }

      @Override
      public Integer getCurrentDomainGroupVersion() throws IOException {
        return currentVersion;
      }

      @Override
      public boolean isDeletable() throws IOException {
        return deletable;
      }

      @Override
      public void setDeletable(boolean deletable) throws IOException {
        this.deletable = deletable;
      }

      public void removeCount(String countID) throws IOException {
      }

      @Override
      public void setCount(String countID, long count) throws IOException {
      }

      @Override
      public Long getCount(String countID) throws IOException {
        return null;
      }

      @Override
      public Set<String> getCountKeys() throws IOException {
        return null;
      }

      @Override
      public void delete() throws IOException {
      }
    };
    private final MockHostDomainPartition PARTITION_FOR_DELETION = new MockHostDomainPartition(1, 0, 0) {
      @Override
      public Integer getUpdatingToDomainGroupVersion() throws IOException {
        return null;
      }
    };

    private MockHostDomain getMockHostDomain(Domain domain) {
      return new MockHostDomain(domain) {

        @Override
        public Set<HostDomainPartition> getPartitions() throws IOException {
          Set<HostDomainPartition> partitions = new HashSet<HostDomainPartition>();
          partitions.add(HOST_DOMAIN_PARTITION);
          partitions.add(PARTITION_FOR_DELETION);
          return partitions;
        }

        @Override
        public HostDomainPartition addPartition(int partNum, int initialVersion) {
          return null;
        }
      };
    }

    private Host getMockHost(final HostDomain hostDomain) {
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
              Arrays.asList(new AbstractDomainVersion() {
                @Override
                public void setDefunct(boolean isDefunct) throws IOException {
                }

                @Override
                public boolean isDefunct() throws IOException {
                  return true;
                }

                @Override
                public int getVersionNumber() {
                  return 45;
                }

                @Override
                public Set<PartitionInfo> getPartitionInfos() throws IOException {
                  return null;
                }

                @Override
                public Long getClosedAt() throws IOException {
                  return null;
                }

                @Override
                public void close() throws IOException {
                }

                @Override
                public void cancel() throws IOException {
                }

                @Override
                public void addPartitionInfo(int partNum, long numBytes,
                                             long numRecords) throws IOException {
                }
              }));
        }
      };
    }

    private DomainGroupVersion getMockDomainGroupVersion(final Domain domain) {
      // the domain version for this domain group version will be 0
      final DomainGroupVersionDomainVersion dgvdv =
          new MockDomainGroupVersionDomainVersion(domain, 0);
      // the domain group version number is 1. note the difference between dgv
      // and dgvdv's version numbers - this is intentional
      return new MockDomainGroupVersion(Collections.singleton(dgvdv), null, 1);
    }

    private DomainGroup getMockDomainGroup(final Domain domain) {
      return new MockDomainGroup("myDomainGroup") {
        private DomainGroupVersion dgv = getMockDomainGroupVersion(domain);
        private SortedSet<DomainGroupVersion> versions = new TreeSet<DomainGroupVersion>() {{
          add(dgv);
        }};

        @Override
        public SortedSet<DomainGroupVersion> getVersions() {
          return versions;
        }

        @Override
        public DomainGroupVersion getVersionByNumber(int versionNumber) throws IOException {
          return dgv;
        }
      };
    }
  }

  public void testUpdate() throws Exception {
    final MockUpdater mockUpdater = new MockUpdater();

    StorageEngine mockStorageEngine = fixtures.getMockStorageEngine(mockUpdater);
    Domain mockDomain = fixtures.getMockDomain(mockStorageEngine);
    HostDomain mockHostDomain = fixtures.getMockHostDomain(mockDomain);
    Host mockHost = fixtures.getMockHost(mockHostDomain);
    Ring mockRing = fixtures.getMockRing(mockHost);
    DomainGroup mockDomainGroup = fixtures.getMockDomainGroup(mockDomain);
    RingGroup mockRingGroup = fixtures.getMockRingGroup(mockDomainGroup, mockRing);

    UpdateManager ud = new UpdateManager(new MockPartitionServerConfigurator(1,
        null, "myRingGroup", "/local/data/dir"), mockHost,
        mockRingGroup, mockRing);
    ud.update();
    assertTrue("update() was called on the storage engine",
        mockUpdater.isUpdated());
    assertEquals("excludeVersions passed in as expected",
        Collections.singleton(45), mockUpdater.excludeVersions);
    assertEquals("update() called with proper args", Integer.valueOf(0),
        mockUpdater.updatedToVersion);
    assertEquals("current version", Integer.valueOf(1),
        fixtures.HOST_DOMAIN_PARTITION.getCurrentDomainGroupVersion());
    assertNull("updating to version",
        fixtures.HOST_DOMAIN_PARTITION.getUpdatingToDomainGroupVersion());

    assertFalse("host domain contains the partition", fixtures.MOCK_DELETER.hasDeleted());
    assertFalse("host domain partition has not yet been deleted",
        fixtures.PARTITION_FOR_DELETION.isDeleted());

    // Test partition deletion
    fixtures.PARTITION_FOR_DELETION.setDeletable(true);
    ud.update();

    assertTrue("host domain does not contain the partition",
        fixtures.MOCK_DELETER.hasDeleted());
    assertTrue("host domain partition has been deleted",
        fixtures.PARTITION_FOR_DELETION.isDeleted());
  }

  public void testGarbageCollectDomain() throws Exception {
    final MockUpdater mockUpdater = new MockUpdater();

    StorageEngine mockStorageEngine = fixtures.getMockStorageEngine(mockUpdater);
    Domain mockDomain = fixtures.getMockDomain(mockStorageEngine);
    MockHostDomain mockHostDomain = fixtures.getMockHostDomain(mockDomain);
    Host mockHost = fixtures.getMockHost(mockHostDomain);
    Ring mockRing = fixtures.getMockRing(mockHost);

    // Empty domain group version
    DomainGroup mockDomainGroup = new MockDomainGroup("myDomainGroup") {
      SortedSet<DomainGroupVersion> versions = new TreeSet<DomainGroupVersion>() {{
        add(new MockDomainGroupVersion(Collections.<DomainGroupVersionDomainVersion>emptySet(), null, 0));
      }};

      @Override
      public SortedSet<DomainGroupVersion> getVersions() {
        return versions;
      }
    };

    RingGroup mockRingGroup = fixtures.getMockRingGroup(mockDomainGroup, mockRing);

    UpdateManager ud = new UpdateManager(new MockPartitionServerConfigurator(1,
        null, "myRingGroup", "/local/data/dir"), mockHost,
        mockRingGroup, mockRing);

    ud.update();

    assertFalse("update() was not called on the storage engine",
        mockUpdater.isUpdated());

    assertTrue("host domain does not contain the partition", fixtures.MOCK_DELETER.hasDeleted());
    assertTrue("host domain partition has been deleted", fixtures.PARTITION_FOR_DELETION.isDeleted());
    assertTrue("host domain has been deleted", mockHostDomain.isDeleted());
  }

  public void testFailedUpdateTask() throws Exception {
    final MockUpdater failingUpdater = new MockUpdater() {
      @Override
      public void update(int toVersion, Set<Integer> excludeVersions) throws IOException {
        super.update(toVersion, excludeVersions);
        throw new IOException("Failed to update.");
      }
    };

    StorageEngine mockStorageEngine = fixtures.getMockStorageEngine(failingUpdater);
    Domain mockDomain = fixtures.getMockDomain(mockStorageEngine);
    HostDomain mockHostDomain = fixtures.getMockHostDomain(mockDomain);
    Host mockHost = fixtures.getMockHost(mockHostDomain);
    Ring mockRing = fixtures.getMockRing(mockHost);
    DomainGroup mockDomainGroup = fixtures.getMockDomainGroup(mockDomain);
    RingGroup mockRingGroup = fixtures.getMockRingGroup(mockDomainGroup, mockRing);

    UpdateManager ud = new UpdateManager(new MockPartitionServerConfigurator(1,
        null, "myRingGroup", "/local/data/dir"), mockHost,
        mockRingGroup, mockRing);

    try {
      ud.update();
      assertTrue("update() should have been called on the storage engine", failingUpdater.isUpdated());
      fail("Should throw an IOException when a task update fails.");
    } catch (IOException e) {
      // Correct behavior
    }
  }

  public void testInterruptedUpdateTask() throws Exception {
    final MockUpdater mockUpdater = new MockUpdater();

    StorageEngine mockStorageEngine = fixtures.getMockStorageEngine(mockUpdater);
    Domain mockDomain = fixtures.getMockDomain(mockStorageEngine);
    HostDomain mockHostDomain = fixtures.getMockHostDomain(mockDomain);
    Host mockHost = fixtures.getMockHost(mockHostDomain);
    Ring mockRing = fixtures.getMockRing(mockHost);
    DomainGroup mockDomainGroup = fixtures.getMockDomainGroup(mockDomain);
    RingGroup mockRingGroup = fixtures.getMockRingGroup(mockDomainGroup, mockRing);

    UpdateManager ud = new UpdateManager(new MockPartitionServerConfigurator(1,
        null, "myRingGroup", "/local/data/dir"), mockHost,
        mockRingGroup, mockRing);

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
