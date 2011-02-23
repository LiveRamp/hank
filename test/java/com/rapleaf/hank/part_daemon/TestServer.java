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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;

import org.apache.thrift.TException;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.DaemonState;
import com.rapleaf.hank.coordinator.DaemonType;
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
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.PartDaemon.Iface;
import com.rapleaf.hank.partitioner.ConstantPartitioner;
import com.rapleaf.hank.storage.MockStorageEngine;
import com.rapleaf.hank.storage.StorageEngine;

public class TestServer extends BaseTestCase {
  private static final RingConfig mockRingConfig = new MockRingConfig(null, null, 0, null) {
//    @Override
//    public Set<Integer> getDomainPartitionsForHost(
//        PartDaemonAddress hostAndPort, int domainId)
//    throws DataNotFoundException {
//      return Collections.singleton(0);
//    }
  };

  public void testColdStartAndShutDown() throws Exception {
    final MockStorageEngine mockStorageEngine = new MockStorageEngine();

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

    MockPartDaemonConfigurator mockConfigurator = new MockPartDaemonConfigurator(12345, mockCoordinator, "myRingGroup", 1, "/tmp/local_data_dir");

    Server server = new Server(mockConfigurator) {

      @Override
      protected Iface getHandler() throws DataNotFoundException, IOException {
        return new Iface() {
          @Override
          public HankResponse get(byte domainId, ByteBuffer key) throws TException {return null;}
        };
      }
    };

    // should move smoothly from startable to idle
    server.onDaemonStateChange(null, 0, null, null, DaemonState.STARTABLE);
    assertEquals("Daemon state is now STARTED",
        DaemonState.STARTED,
        mockCoordinator.getDaemonState(null, 0, new PartDaemonAddress("localhost", 12345), null));

    // duplicate startable should end up back in started without any mess
    server.onDaemonStateChange(null, 0, null, null, DaemonState.STARTABLE);
    assertEquals("Daemon state is now STARTED",
        DaemonState.STARTED,
        mockCoordinator.getDaemonState(null, 0, new PartDaemonAddress("localhost", 12345), null));

    server.onDaemonStateChange(null, 0, null, null, DaemonState.STOPPABLE);
    assertEquals("Daemon state is now IDLE",
        DaemonState.IDLE,
        mockCoordinator.getDaemonState(null, 0, new PartDaemonAddress("localhost", 12345), null));
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
