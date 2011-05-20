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
import java.util.Collections;
import java.util.Set;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.config.PartservConfigurator;
import com.rapleaf.hank.coordinator.AbstractHostDomainPartition;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.DomainGroupVersion;
import com.rapleaf.hank.coordinator.DomainGroupVersionDomainVersion;
import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostDomain;
import com.rapleaf.hank.coordinator.HostDomainPartition;
import com.rapleaf.hank.coordinator.MockDomainGroup;
import com.rapleaf.hank.coordinator.MockDomainGroupVersion;
import com.rapleaf.hank.coordinator.MockDomainGroupVersionDomainVersion;
import com.rapleaf.hank.coordinator.MockHost;
import com.rapleaf.hank.coordinator.MockRingConfig;
import com.rapleaf.hank.coordinator.MockRingGroupConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.partitioner.ConstantPartitioner;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.Updater;
import com.rapleaf.hank.storage.mock.MockStorageEngine;

public class TestUpdateManager extends BaseTestCase {
  private final class MRG extends MockRingGroupConfig {
    private MRG(DomainGroup dcg, String name, Set<RingConfig> ringConfigs) {
      super(dcg, name, ringConfigs);
    }

    @Override
    public RingConfig getRingConfigForHost(PartDaemonAddress hostAddress) {
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

  private static final Host mockHostConfig = new MockHost(new PartDaemonAddress("localhost", 1)) {
    @Override
    public HostDomain getDomainById(int domainId) {
      return new HostDomain() {
        @Override
        public Set<HostDomainPartition> getPartitions() throws IOException {
          return Collections.singleton((HostDomainPartition)new AbstractHostDomainPartition() {
            @Override
            public void setUpdatingToDomainGroupVersion(Integer version)
            throws IOException {}

            @Override
            public void setCurrentDomainGroupVersion(int version) throws IOException {}

            @Override
            public Integer getUpdatingToDomainGroupVersion() throws IOException {
              return 2;
            }

            @Override
            public int getPartNum() {
              return 0;
            }

            @Override
            public Integer getCurrentDomainGroupVersion() throws IOException {
              return 1;
            }
          });
        }

        @Override
        public int getDomainId() {
          return 1;
        }

        @Override
        public HostDomainPartition addPartition(int partNum, int initialVersion) {return null;}
      };
    }
  };

  private static final RingConfig mockRingConfig = new MockRingConfig(null, null, 0, null) {
    @Override
    public Host getHostConfigByAddress(PartDaemonAddress address) {
      return mockHostConfig;
    }
  };

  public void testUpdate() throws Exception {
    final MockUpdater mockUpdater = new MockUpdater();

    StorageEngine mockStorageEngine = new MSE(mockUpdater);

    DomainGroup mockDomainGroupConfig = getMockDomainGroupConfig(mockStorageEngine);

    final RingGroupConfig mockRingGroupConfig = new MRG(mockDomainGroupConfig, "myRingGroup", null);

    UpdateManager ud = new UpdateManager(new MockPartDaemonConfigurator(1, null, "myRingGroup", "/local/data/dir"), mockHostConfig, mockRingGroupConfig, mockRingConfig);
    ud.update();
    assertTrue("update() was called on the storage engine", mockUpdater.isUpdated());
  }

  private static DomainGroupVersion getMockDomainGroupConfigVersion(
      final StorageEngine mockStorageEngine) 
  {
    return new MockDomainGroupVersion(Collections.singleton(
        (DomainGroupVersionDomainVersion)new MockDomainGroupVersionDomainVersion(
            new MockDomain("myDomain",
                1,
                new ConstantPartitioner(),
                mockStorageEngine,
                0),
            0)),
        null,
        0);
  }

  private DomainGroup getMockDomainGroupConfig(
      final StorageEngine mockStorageEngine) {
    DomainGroup mockDomainGroupConfig = new MockDomainGroup("myDomainGroup") {
      @Override
      public DomainGroupVersion getLatestVersion() {
        return getMockDomainGroupConfigVersion(mockStorageEngine);
      }
    };
    return mockDomainGroupConfig;
  }
}
