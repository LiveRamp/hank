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
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.coordinator.mock.MockCoordinator;
import com.rapleaf.hank.generated.HankResponse;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TestPartitionServer extends BaseTestCase {
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

  private static final MockHost mockHost = new MockHost(new PartitionServerAddress("localhost", 1));

  private static final Ring mockRing = new MockRing(null, null, 0, null) {
    @Override
    public Host getHostByAddress(PartitionServerAddress address) {
      return mockHost;
    }
  };

  private static final RingGroup mockRingGroup = new MockRingGroup(null, "myRingGroup", null) {
    @Override
    public Ring getRingForHost(PartitionServerAddress hostAddress) {
      return mockRing;
    }
  };

  private static final MockCoordinator mockCoord = new MockCoordinator() {
    @Override
    public RingGroup getRingGroup(String ringGroupName) {
      return mockRingGroup;
    }
  };

  private static final MockPartitionServerConfigurator CONFIGURATOR = new MockPartitionServerConfigurator(12345, mockCoord, "myRingGroup", null);

  public void testColdStartAndShutDown() throws Exception {
    final MockUpdateManager mockUpdateManager = new MockUpdateManager();
    final PartitionServer partitionServer = new PartitionServer(CONFIGURATOR, "localhost") {
      @Override
      protected IfaceWithShutdown getHandler() throws IOException {
        return new IfaceWithShutdown() {
          @Override
          public HankResponse get(int domainId, ByteBuffer key) throws TException {
            return null;
          }

          @Override
          public void shutDown() throws InterruptedException {
          }
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
          partitionServer.run();
        } catch (IOException e) {
          fail("exception!" + e);
        }
      }
    };
    Thread t = new Thread(serverRunnable, "PartitionServer thread");

    // TODO: test here for when starting up with commands in the queue...

    t.start();
    Thread.sleep(1000);
    assertEquals(HostState.IDLE, mockHost.getState());

    // should move smoothly from startable to idle
    mockHost.enqueueCommand(HostCommand.SERVE_DATA);
    partitionServer.onCommandQueueChange(mockHost);
    assertEquals(HostState.SERVING, mockHost.getState());

    mockHost.enqueueCommand(HostCommand.GO_TO_IDLE);
    partitionServer.onCommandQueueChange(mockHost);
    assertEquals(HostState.IDLE, mockHost.getState());

    mockHost.enqueueCommand(HostCommand.EXECUTE_UPDATE);
    partitionServer.onCommandQueueChange(mockHost);
    assertEquals(HostState.UPDATING, mockHost.getState());

    Thread.sleep(1500);

    assertTrue("Update called", mockUpdateManager.updateCalled);
    assertNull("Current command cleared", mockHost.getCurrentCommand());
    assertEquals(HostState.IDLE, mockHost.getState());

    partitionServer.stop();
    assertEquals(HostState.IDLE, mockHost.getState());

    t.join();
    assertEquals(HostState.OFFLINE, mockHost.getState());
  }
}
