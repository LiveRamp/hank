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
import com.rapleaf.hank.coordinator.MockCoordinator;
import com.rapleaf.hank.coordinator.MockDomainConfig;
import com.rapleaf.hank.coordinator.MockDomainConfigVersion;
import com.rapleaf.hank.coordinator.MockDomainGroupConfig;
import com.rapleaf.hank.coordinator.MockDomainGroupConfigVersion;
import com.rapleaf.hank.coordinator.MockHostConfig;
import com.rapleaf.hank.coordinator.MockRingConfig;
import com.rapleaf.hank.coordinator.MockRingGroupConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.partitioner.ConstantPartitioner;
import com.rapleaf.hank.storage.MockStorageEngine;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.Updater;

public class TestUpdateManager extends BaseTestCase {
  private final class MC extends MockCoordinator {
    private final RingGroupConfig mockRingGroupConfig;

    private MC(RingGroupConfig mockRingGroupConfig) {
      this.mockRingGroupConfig = mockRingGroupConfig;
    }

    @Override
    public RingGroupConfig getRingGroupConfig(String ringGroupName)
        throws DataNotFoundException {
      return mockRingGroupConfig;
    }
  }

  private final class MRG extends MockRingGroupConfig {
    private MRG(DomainGroupConfig dcg, String name, Set<RingConfig> ringConfigs) {
      super(dcg, name, ringConfigs);
    }

    @Override
    public RingConfig getRingConfigForHost(PartDaemonAddress hostAddress)
        throws DataNotFoundException {
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
        public HostDomainPartitionConfig addPartition(int partNum, int initialVersion) throws Exception {return null;}
      };
    }
  };

  private static final RingConfig mockRingConfig = new MockRingConfig(null, null, 0, null) {
    @Override
    public HostConfig getHostConfigByAddress(PartDaemonAddress address) {
      return mockHostConfig;
    }
  };

  public void testColdStart() throws Exception {
    final MockUpdater mockUpdater = new MockUpdater();

    StorageEngine mockStorageEngine = new MSE(mockUpdater);

    DomainGroupConfig mockDomainGroupConfig = getMockDomainGroupConfig(mockStorageEngine);

    final RingGroupConfig mockRingGroupConfig = new MRG(mockDomainGroupConfig, "myRingGroup", null);

    MockCoordinator mockCoordinator = new MC(mockRingGroupConfig);

    UpdateManager ud = new UpdateManager(new MockPartDaemonConfigurator(1, mockCoordinator, "myRingGroup", "/local/data/dir"), "localhost");

    // should move smoothly from updateable to idle
    mockHostConfig.setUpdateDaemonState(UpdateDaemonState.UPDATABLE);
    ud.onHostStateChange(mockHostConfig);
    assertEquals("Daemon state is now in IDLE",
        UpdateDaemonState.IDLE,
        mockHostConfig.getUpdateDaemonState());
    assertTrue("update() was called on the storage engine", mockUpdater.isUpdated());
  }

  public void testRestartsUpdating() throws Exception {
    final MockUpdater mockUpdater = new MockUpdater();

    StorageEngine mockStorageEngine = new MSE(mockUpdater);

    DomainGroupConfig mockDomainGroupConfig = getMockDomainGroupConfig(mockStorageEngine);

    final RingGroupConfig mockRingGroupConfig = new MRG(mockDomainGroupConfig, "myRingGroup", null);

    MockCoordinator mockCoordinator = new MC(mockRingGroupConfig);
    mockHostConfig.setUpdateDaemonState(UpdateDaemonState.UPDATING);

    final UpdateManager ud = new UpdateManager(new MockPartDaemonConfigurator(1, mockCoordinator, "myRingGroup", null), "localhost");
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          ud.run();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }).start();
    Thread.sleep(100);

    // should move smoothly from updateable to idle
    assertEquals("Daemon state is now in IDLE",
        UpdateDaemonState.IDLE,
        mockHostConfig.getUpdateDaemonState());
    assertTrue("update() was called on the storage engine", mockUpdater.isUpdated());
    assertTrue(mockHostConfig.isUpdateDaemonOnline());
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
