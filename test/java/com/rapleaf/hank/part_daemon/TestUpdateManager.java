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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.config.PartservConfigurator;
import com.rapleaf.hank.coordinator.AbstractDomainVersion;
import com.rapleaf.hank.coordinator.AbstractHostDomain;
import com.rapleaf.hank.coordinator.AbstractHostDomainPartition;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.DomainGroupVersion;
import com.rapleaf.hank.coordinator.DomainGroupVersionDomainVersion;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostDomain;
import com.rapleaf.hank.coordinator.HostDomainPartition;
import com.rapleaf.hank.coordinator.MockDomainGroup;
import com.rapleaf.hank.coordinator.MockDomainGroupVersion;
import com.rapleaf.hank.coordinator.MockDomainGroupVersionDomainVersion;
import com.rapleaf.hank.coordinator.MockHost;
import com.rapleaf.hank.coordinator.MockRing;
import com.rapleaf.hank.coordinator.MockRingGroup;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.PartitionInfo;
import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.partitioner.ConstantPartitioner;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.Updater;
import com.rapleaf.hank.storage.mock.MockStorageEngine;

public class TestUpdateManager extends BaseTestCase {
  private final class MRG extends MockRingGroup {
    private MRG(DomainGroup dcg, String name, Set<Ring> ringConfigs) {
      super(dcg, name, ringConfigs);
    }

    @Override
    public Ring getRingForHost(PartDaemonAddress hostAddress) {
      return mockRingConfig;
    }
  }

  private final class MSE extends MockStorageEngine {
    private final MockUpdater mockUpdater;

    private MSE(MockUpdater mockUpdater) {
      this.mockUpdater = mockUpdater;
    }

    @Override
    public Updater getUpdater(PartservConfigurator configurator, int partNum) {
      return mockUpdater;
    }
  }

  private static final HostDomainPartition HOST_DOMAIN_PARTITION = new AbstractHostDomainPartition() {
    private Integer updatingToVersion = 1;
    private Integer currentVersion = 0;

    @Override
    public void setUpdatingToDomainGroupVersion(Integer version) throws IOException {
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
    public int getPartNum() {
      return 0;
    }

    @Override
    public Integer getCurrentDomainGroupVersion() throws IOException {
      return currentVersion;
    }

    @Override
    public void removeCount(String countID) throws IOException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void setCount(String countID, long count) throws IOException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public Long getCount(String countID) throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Set<String> getCountKeys() throws IOException {
      // TODO Auto-generated method stub
      return null;
    }
  };

  private static final HostDomain hostDomain = new AbstractHostDomain() {
    @Override
    public Set<HostDomainPartition> getPartitions() throws IOException {
      return Collections.singleton((HostDomainPartition) HOST_DOMAIN_PARTITION);
    }

    @Override
    public int getDomainId() {
      return 1;
    }

    @Override
    public HostDomainPartition addPartition(int partNum, int initialVersion) {
      return null;
    }
  };

  private static final Host mockHostConfig = new MockHost(new PartDaemonAddress("localhost", 1)) {
    @Override
    public HostDomain getDomainById(int domainId) {
      return hostDomain;
    }
  };

  private static final Ring mockRingConfig = new MockRing(null, null, 0, null) {
    @Override
    public Host getHostByAddress(PartDaemonAddress address) {
      return mockHostConfig;
    }
  };

  public void testUpdate() throws Exception {
    final MockUpdater mockUpdater = new MockUpdater();

    StorageEngine mockStorageEngine = new MSE(mockUpdater);

    DomainGroup mockDomainGroupConfig = getMockDomainGroupConfig(mockStorageEngine);

    final RingGroup mockRingGroupConfig = new MRG(mockDomainGroupConfig, "myRingGroup", null);

    UpdateManager ud = new UpdateManager(new MockPartDaemonConfigurator(1, null, "myRingGroup", "/local/data/dir"), mockHostConfig, mockRingGroupConfig, mockRingConfig);
    ud.update();
    assertTrue("update() was called on the storage engine", mockUpdater.isUpdated());
    assertEquals("excludeVersions passed in as expected", Collections.singleton(45), mockUpdater.excludeVersions);
    assertEquals("update() called with proper args", Integer.valueOf(0), mockUpdater.updatedToVersion);
    assertEquals("current version", Integer.valueOf(1), HOST_DOMAIN_PARTITION.getCurrentDomainGroupVersion());
    assertNull("updating to version", HOST_DOMAIN_PARTITION.getUpdatingToDomainGroupVersion());
  }

  private static DomainGroupVersion getMockDomainGroupConfigVersion(final StorageEngine mockStorageEngine) {
    final MockDomain domain = new MockDomain("myDomain", 1, new ConstantPartitioner(), mockStorageEngine, null, null) {
      @Override
      public SortedSet<DomainVersion> getVersions() {
        return new TreeSet<DomainVersion>(Arrays.asList(new AbstractDomainVersion() {
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
          public void addPartitionInfo(int partNum, long numBytes, long numRecords) throws IOException {
          }
        }));
      }
    };
    // the domain version for this domain group version will be 0
    final DomainGroupVersionDomainVersion dgvdv = new MockDomainGroupVersionDomainVersion(domain, 0);
    // the domain group version number is 1. note the difference between dgv
    // and dgvdv's version numbers - this is intentional
    return new MockDomainGroupVersion(Collections.singleton(dgvdv), null, 1);
  }

  private DomainGroup getMockDomainGroupConfig(final StorageEngine mockStorageEngine) {
    DomainGroup mockDomainGroupConfig = new MockDomainGroup("myDomainGroup") {
      @Override
      public DomainGroupVersion getLatestVersion() {
        return getMockDomainGroupConfigVersion(mockStorageEngine);
      }
    };
    return mockDomainGroupConfig;
  }
}
