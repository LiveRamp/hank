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
import com.rapleaf.hank.coordinator.DomainConfigVersion;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfigVersion;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostDomainConfig;
import com.rapleaf.hank.coordinator.HostDomainPartitionConfig;
import com.rapleaf.hank.coordinator.MockDomainConfigVersion;
import com.rapleaf.hank.coordinator.MockDomainGroupConfig;
import com.rapleaf.hank.coordinator.MockDomainGroupConfigVersion;
import com.rapleaf.hank.coordinator.MockHostConfig;
import com.rapleaf.hank.coordinator.MockRingConfig;
import com.rapleaf.hank.coordinator.MockRingGroupConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.coordinator.mock.MockDomainConfig;
import com.rapleaf.hank.partitioner.ConstantPartitioner;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.Updater;
import com.rapleaf.hank.storage.mock.MockStorageEngine;

public class TestUpdateManager extends BaseTestCase {
  private final class MRG extends MockRingGroupConfig {
    private MRG(DomainGroupConfig dcg, String name, Set<RingConfig> ringConfigs) {
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

  private static final HostConfig mockHostConfig = new MockHostConfig(new PartDaemonAddress("localhost", 1)) {
    @Override
    public HostDomainConfig getDomainById(int domainId) {
      return new HostDomainConfig() {
        @Override
        public Set<HostDomainPartitionConfig> getPartitions() throws IOException {
          return Collections.singleton((HostDomainPartitionConfig)new HostDomainPartitionConfig() {
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
        public HostDomainPartitionConfig addPartition(int partNum, int initialVersion) {return null;}
      };
    }
  };

  private static final RingConfig mockRingConfig = new MockRingConfig(null, null, 0, null) {
    @Override
    public HostConfig getHostConfigByAddress(PartDaemonAddress address) {
      return mockHostConfig;
    }
  };

  public void testUpdate() throws Exception {
    final MockUpdater mockUpdater = new MockUpdater();

    StorageEngine mockStorageEngine = new MSE(mockUpdater);

    DomainGroupConfig mockDomainGroupConfig = getMockDomainGroupConfig(mockStorageEngine);

    final RingGroupConfig mockRingGroupConfig = new MRG(mockDomainGroupConfig, "myRingGroup", null);

    UpdateManager ud = new UpdateManager(new MockPartDaemonConfigurator(1, null, "myRingGroup", "/local/data/dir"), mockHostConfig, mockRingGroupConfig, mockRingConfig);
    ud.update();
    assertTrue("update() was called on the storage engine", mockUpdater.isUpdated());
  }

  private static DomainGroupConfigVersion getMockDomainGroupConfigVersion(
      final StorageEngine mockStorageEngine) 
  {
    return new MockDomainGroupConfigVersion(Collections.singleton(
        (DomainConfigVersion)new MockDomainConfigVersion(
            new MockDomainConfig("myDomain",
                1,
                new ConstantPartitioner(),
                mockStorageEngine,
                0),
            0)),
        null,
        0);
  }

  private DomainGroupConfig getMockDomainGroupConfig(
      final StorageEngine mockStorageEngine) {
    DomainGroupConfig mockDomainGroupConfig = new MockDomainGroupConfig("myDomainGroup") {
      @Override
      public DomainGroupConfigVersion getLatestVersion() {
        return getMockDomainGroupConfigVersion(mockStorageEngine);
      }
    };
    return mockDomainGroupConfig;
  }
}
