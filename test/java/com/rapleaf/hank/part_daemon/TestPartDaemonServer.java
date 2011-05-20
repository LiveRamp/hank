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
import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.MockHost;
import com.rapleaf.hank.coordinator.MockRing;
import com.rapleaf.hank.coordinator.MockRingGroup;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.coordinator.mock.MockCoordinator;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.PartDaemon.Iface;

public class TestPartDaemonServer extends BaseTestCase {
  private final class MockUpdateManager implements IUpdateManager {
    public boolean updateCalled = false;
    @Override
    public void update() throws IOException {
      updateCalled = true;
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private static final MockHost mockHostConfig = new MockHost(new PartDaemonAddress("localhost", 1));

  private static final Ring mockRingConfig = new MockRing(null, null, 0, null) {
    @Override
    public Host getHostConfigByAddress(PartDaemonAddress address) {
      return mockHostConfig;
    }
  };

  private static final RingGroup mockRingGroupConfig = new MockRingGroup(null, "myRingGroup", null) {
    @Override
    public Ring getRingConfigForHost(PartDaemonAddress hostAddress) {
      return mockRingConfig;
    }
  };

  private static final MockCoordinator mockCoord = new MockCoordinator() {
    @Override
    public RingGroup getRingGroupConfig(String ringGroupName) {
      return mockRingGroupConfig;
    }
  };

  private static final MockPartDaemonConfigurator configurator = new MockPartDaemonConfigurator(12345, mockCoord, "myRingGroup", null);

  public void testColdStartAndShutDown() throws Exception {
    final MockUpdateManager mockUpdateManager = new MockUpdateManager();
    final PartDaemonServer server = new PartDaemonServer(configurator, "localhost") {
      @Override
      protected Iface getHandler() throws IOException {
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

    // TODO: test here for when starting up with commands in the queue...

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
    assertNull("current command cleared", mockHostConfig.getCurrentCommand());

    server.stop();
    t.join();
    assertEquals(HostState.OFFLINE, mockHostConfig.getState());
  }
}
