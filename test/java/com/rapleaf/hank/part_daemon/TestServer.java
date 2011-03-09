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

import org.apache.thrift.TException;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.MockCoordinator;
import com.rapleaf.hank.coordinator.MockHostConfig;
import com.rapleaf.hank.coordinator.MockRingConfig;
import com.rapleaf.hank.coordinator.MockRingGroupConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.PartDaemon.Iface;

public class TestServer extends BaseTestCase {
  private final class MockUpdateManager implements IUpdateManager {
    public boolean updateCalled = false;
    @Override
    public void update() throws DataNotFoundException, IOException {
      updateCalled = true;
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  private static final MockHostConfig mockHostConfig = new MockHostConfig(new PartDaemonAddress("localhost", 1));

  private static final RingConfig mockRingConfig = new MockRingConfig(null, null, 0, null) {
    @Override
    public HostConfig getHostConfigByAddress(PartDaemonAddress address) {
      return mockHostConfig;
    }
  };

  private static final RingGroupConfig mockRingGroupConfig = new MockRingGroupConfig(null, "myRingGroup", null) {
    @Override
    public RingConfig getRingConfigForHost(PartDaemonAddress hostAddress) {
      return mockRingConfig;
    }
  };

  private static final MockCoordinator mockCoord = new MockCoordinator() {
    @Override
    public RingGroupConfig getRingGroupConfig(String ringGroupName) {
      return mockRingGroupConfig;
    }
  };

  private static final MockPartDaemonConfigurator configurator = new MockPartDaemonConfigurator(12345, mockCoord, "myRingGroup", null);

  public void testColdStartAndShutDown() throws Exception {
    final MockUpdateManager mockUpdateManager = new MockUpdateManager();
    final Server server = new Server(configurator, "localhost") {
      @Override
      protected Iface getHandler() throws DataNotFoundException, IOException {
        return new Iface() {
          @Override
          public HankResponse get(int domainId, ByteBuffer key) throws TException {return null;}
        };
      }

      @Override
      protected IUpdateManager getUpdateManager() {
        return mockUpdateManager;
      }
    };

    Runnable serverRunnable = new Runnable() {
      @Override
      public void run() {
        try {
          server.run();
        } catch (IOException e) {
          fail("exception!" + e);
        }
      }
    };
    Thread t = new Thread(serverRunnable, "server thread");

//    mockHostConfig.enqueueCommand(HostCommand.GO_TO_IDLE);
    t.start();
    Thread.sleep(1000);
    assertEquals(HostState.IDLE, mockHostConfig.getState());

    // should move smoothly from startable to idle
    mockHostConfig.enqueueCommand(HostCommand.SERVE_DATA);
    server.onCommandQueueChange(mockHostConfig);
    assertEquals("Daemon state is now SERVING",
        HostState.SERVING,
        mockHostConfig.getState());

    mockHostConfig.enqueueCommand(HostCommand.GO_TO_IDLE);
    server.onCommandQueueChange(mockHostConfig);
    assertEquals("Daemon state is now IDLE",
        HostState.IDLE,
        mockHostConfig.getState());

    mockHostConfig.enqueueCommand(HostCommand.EXECUTE_UPDATE);
    server.onCommandQueueChange(mockHostConfig);
    assertEquals("Daemon state is now UPDATING",
        HostState.UPDATING,
        mockHostConfig.getState());
    Thread.sleep(1500);
    assertTrue("update called", mockUpdateManager.updateCalled);

    server.stop();
    t.join();
    assertEquals(HostState.OFFLINE, mockHostConfig.getState());
  }
}
