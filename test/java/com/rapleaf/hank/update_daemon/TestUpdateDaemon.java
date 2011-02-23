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
package com.rapleaf.hank.update_daemon;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.EnumSet;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.config.MockUpdateDaemonConfigurator;
import com.rapleaf.hank.config.UpdateDaemonConfigurator;
import com.rapleaf.hank.coordinator.DomainConfigVersion;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
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
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.partitioner.ConstantPartitioner;
import com.rapleaf.hank.storage.MockStorageEngine;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.Updater;

public class TestUpdateDaemon extends BaseTestCase {
  private static final RingConfig mockRingConfig = new MockRingConfig(null, null, 0, null) {
//    @Override
//    public Set<Integer> getDomainPartitionsForHost(
//        PartDaemonAddress hostAndPort, int domainId)
//    throws DataNotFoundException {
//      return Collections.singleton(0);
//    }
  };

  public void testColdStart() throws Exception {
    final MockUpdater mockUpdater = new MockUpdater();

    final StorageEngine mockStorageEngine = new MockStorageEngine() {
      @Override
      public Updater getUpdater(UpdateDaemonConfigurator configurator, int partNum) {
        return mockUpdater;
      }
    };

    DomainGroupConfig mockDomainGroupConfig = getMockDomainGroupConfig(mockStorageEngine);

    final RingGroupConfig mockRingGroupConfig = new MockRingGroupConfig(mockDomainGroupConfig, "myRingGroup", null) {
      @Override
      public RingConfig getRingConfigForHost(PartDaemonAddress hostAddress)
          throws DataNotFoundException {
        return mockRingConfig;
      }
    };

    MockCoordinator mockCoordinator = new MockCoordinator() {
      private DaemonState daemonState;

      @Override
      public DaemonState getDaemonState(String ringGroupName, int ringNumber,
          PartDaemonAddress hostAddress, DaemonType type) {
        return daemonState;
      }

      @Override
      public RingGroupConfig getRingGroupConfig(String ringGroupName)
          throws DataNotFoundException {
        return mockRingGroupConfig;
      }

      @Override
      public void setDaemonState(String ringGroupName, int ringNumber,
          PartDaemonAddress hostAddress, DaemonType type, DaemonState state) {
        daemonState = state;
      }
    };

    MockUpdateDaemonConfigurator mockConfigurator = new MockUpdateDaemonConfigurator(1, null, 12345, mockCoordinator, "myRingGroup", 1);

    UpdateDaemon ud = new UpdateDaemon(mockConfigurator, "localhost");

    // should move smoothly from updateable to idle
    ud.onDaemonStateChange(null, 0, null, null, DaemonState.UPDATEABLE);
    assertEquals("Daemon state is now in IDLE",
        DaemonState.IDLE,
        mockCoordinator.getDaemonState(null, 0, new PartDaemonAddress("localhost", 12345), null));
    assertTrue("update() was called on the storage engine", mockUpdater.isUpdated());
  }

  public void testRestartsUpdating() throws Exception {
    final MockUpdater mockUpdater = new MockUpdater();

    final StorageEngine mockStorageEngine = new MockStorageEngine() {
      @Override
      public Updater getUpdater(UpdateDaemonConfigurator configurator, int partNum) {
        return mockUpdater;
      }
    };

    DomainGroupConfig mockDomainGroupConfig = getMockDomainGroupConfig(mockStorageEngine);

    final RingGroupConfig mockRingGroupConfig = new MockRingGroupConfig(mockDomainGroupConfig, "myRingGroup", null) {
      @Override
      public RingConfig getRingConfigForHost(PartDaemonAddress hostAddress)
          throws DataNotFoundException {
        return mockRingConfig;
      }
    };

    MockCoordinator mockCoordinator = new MockCoordinator() {
      private DaemonState daemonState = DaemonState.UPDATING;

      @Override
      public DaemonState getDaemonState(String ringGroupName, int ringNumber,
          PartDaemonAddress hostAddress, DaemonType type) {
        return daemonState;
      }

      @Override
      public RingGroupConfig getRingGroupConfig(String ringGroupName)
          throws DataNotFoundException {
        return mockRingGroupConfig;
      }

      @Override
      public void setDaemonState(String ringGroupName, int ringNumber,
          PartDaemonAddress hostAddress, DaemonType type, DaemonState state) {
        daemonState = state;
      }
    };

    MockUpdateDaemonConfigurator mockConfigurator = new MockUpdateDaemonConfigurator(1, null, 12345, mockCoordinator, "myRingGroup", 1);

    final UpdateDaemon ud = new UpdateDaemon(mockConfigurator, "localhost");
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          ud.run();
        } catch (UnknownHostException e) {
          throw new RuntimeException(e);
        }
      }
    }).start();
    Thread.sleep(100);

    // should move smoothly from updateable to idle
    assertEquals("Daemon state is now in IDLE",
        DaemonState.IDLE,
        mockCoordinator.getDaemonState(null, 0, new PartDaemonAddress("localhost", 12345), null));
    assertTrue("update() was called on the storage engine", mockUpdater.isUpdated());
  }
  
  public void testDoesNothingOnOtherEvents() throws Exception {
    final MockUpdater mockUpdater = new MockUpdater();

    final StorageEngine mockStorageEngine = new MockStorageEngine() {
      @Override
      public Updater getUpdater(UpdateDaemonConfigurator configurator, int partNum) {
        return mockUpdater;
      }
    };

    DomainGroupConfig mockDomainGroupConfig = getMockDomainGroupConfig(mockStorageEngine);

    final RingGroupConfig mockRingGroupConfig = new MockRingGroupConfig(mockDomainGroupConfig, "myRingGroup", null) {
      @Override
      public RingConfig getRingConfigForHost(PartDaemonAddress hostAddress)
          throws DataNotFoundException {
        return mockRingConfig;
      }
    };

    MockCoordinator mockCoordinator = new MockCoordinator() {
      private DaemonState daemonState;

      @Override
      public DaemonState getDaemonState(String ringGroupName, int ringNumber,
          PartDaemonAddress hostAddress, DaemonType type) {
        return daemonState;
      }

      @Override
      public RingGroupConfig getRingGroupConfig(String ringGroupName)
          throws DataNotFoundException {
        return mockRingGroupConfig;
      }

      @Override
      public void setDaemonState(String ringGroupName, int ringNumber,
          PartDaemonAddress hostAddress, DaemonType type, DaemonState state) {
        daemonState = state;
      }
    };

    MockUpdateDaemonConfigurator mockConfigurator = new MockUpdateDaemonConfigurator(1, null, 12345, mockCoordinator, "myRingGroup", 1);

    UpdateDaemon ud = new UpdateDaemon(mockConfigurator, "localhost");

    for (DaemonState state : EnumSet.complementOf(EnumSet.of(DaemonState.UPDATEABLE, DaemonState.UPDATING))) {
      ud.onDaemonStateChange(null, 0, null, null, state);
      assertEquals("Daemon is unchanged after being probed with " + state,
          null,
          mockCoordinator.getDaemonState(null, 0, new PartDaemonAddress("localhost", 12345), null));
      assertFalse("update() was not called on the storage engine", mockUpdater.isUpdated());
    }
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
