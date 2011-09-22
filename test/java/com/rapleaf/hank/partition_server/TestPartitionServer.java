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
import com.rapleaf.hank.coordinator.mock.MockCoordinator;
import com.rapleaf.hank.generated.HankResponse;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TestPartitionServer extends BaseTestCase {

  private Fixtures fixtures;

  public void setUp() throws Exception {
    super.setUp();
    this.fixtures = new Fixtures();
  }

  private static final class Fixtures {

    static final int PORT_1 = 12345;
    static final int PORT_2 = 12346;
    static final int PORT_3 = 12347;

    // Normal host
    final MockHost host = new MockHost(new PartitionServerAddress("localhost", PORT_1));
    // Failing setState host
    final MockHost failingSetStateHost = new MockHost(new PartitionServerAddress("localhost", PORT_2)) {
      @Override
      public void setState(HostState state) throws IOException {
        super.setState(state);
        if (state == HostState.SERVING) {
          throw new IOException("Failure to set state.");
        }
      }
    };
    // Failing nextCommand host
    final MockHost failingNextCommandHost = new MockHost(new PartitionServerAddress("localhost", PORT_3)) {
      @Override
      public HostCommand nextCommand() throws IOException {
        HostCommand command = super.nextCommand();
        if (command == HostCommand.SERVE_DATA) {
          throw new IOException("Failure to move on to next command.");
        } else {
          return command;
        }
      }
    };

    final Ring mockRing = new MockRing(null, null, 0, null) {
      @Override
      public Host getHostByAddress(PartitionServerAddress address) {
        switch (address.getPortNumber()) {
          case PORT_1:
            return host;
          case PORT_2:
            return failingSetStateHost;
          case PORT_3:
            return failingNextCommandHost;
          default:
            throw new RuntimeException("Unknown host.");
        }
      }
    };

    final RingGroup mockRingGroup = new MockRingGroup(null, "myRingGroup", null) {
      @Override
      public Ring getRingForHost(PartitionServerAddress hostAddress) {
        return mockRing;
      }
    };

    final MockCoordinator mockCoord = new MockCoordinator() {
      @Override
      public RingGroup getRingGroup(String ringGroupName) {
        return mockRingGroup;
      }
    };

    final MockPartitionServerConfigurator CONFIGURATOR1 = new MockPartitionServerConfigurator(PORT_1, mockCoord, "myRingGroup", null);
    final MockPartitionServerConfigurator CONFIGURATOR2 = new MockPartitionServerConfigurator(PORT_2, mockCoord, "myRingGroup", null);
    final MockPartitionServerConfigurator CONFIGURATOR3 = new MockPartitionServerConfigurator(PORT_3, mockCoord, "myRingGroup", null);
  }

  private class MockPartitionServer extends PartitionServer {

    public MockPartitionServer(PartitionServerConfigurator configurator, String hostname) throws IOException {
      super(configurator, hostname);
    }

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
      return null;
    }
  }

  private final class SleepingUpdateManager extends MockUpdateManager {
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

  private final class FailingUpdateManager extends MockUpdateManager {
    public boolean updateFailed = false;

    @Override
    public void update() throws IOException {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      updateFailed = true;
      throw new IOException("Failure");
    }
  }

  public void testColdStartAndShutDown() throws Exception {
    final SleepingUpdateManager updateManager = new SleepingUpdateManager();
    final PartitionServer partitionServer = new MockPartitionServer(fixtures.CONFIGURATOR1, "localhost") {
      @Override
      protected IUpdateManager getUpdateManager() {
        return updateManager;
      }
    };

    Thread thread = createPartitionServerThread(partitionServer);

    thread.start();
    Thread.sleep(1000);
    assertEquals(HostState.IDLE, fixtures.host.getState());

    fixtures.host.enqueueCommand(HostCommand.SERVE_DATA);
    partitionServer.onCommandQueueChange(fixtures.host);
    assertEquals(HostState.SERVING, fixtures.host.getState());

    fixtures.host.enqueueCommand(HostCommand.GO_TO_IDLE);
    partitionServer.onCommandQueueChange(fixtures.host);
    assertEquals(HostState.IDLE, fixtures.host.getState());

    fixtures.host.enqueueCommand(HostCommand.EXECUTE_UPDATE);
    partitionServer.onCommandQueueChange(fixtures.host);
    assertEquals(HostState.UPDATING, fixtures.host.getState());

    Thread.sleep(1500);

    assertTrue("Update called", updateManager.updateCalled);
    assertNull("Current command cleared", fixtures.host.getCurrentCommand());
    assertEquals(HostState.IDLE, fixtures.host.getState());

    partitionServer.stop();

    thread.join();
    assertEquals(HostState.OFFLINE, fixtures.host.getState());
  }

  public void testNonEmptyCommandQueue() throws Exception {
    final SleepingUpdateManager updateManager = new SleepingUpdateManager();
    final PartitionServer partitionServer = new MockPartitionServer(fixtures.CONFIGURATOR1, "localhost") {
      @Override
      protected IUpdateManager getUpdateManager() {
        return updateManager;
      }
    };
    // Enqueue commands
    fixtures.host.enqueueCommand(HostCommand.SERVE_DATA);
    fixtures.host.enqueueCommand(HostCommand.GO_TO_IDLE);
    fixtures.host.enqueueCommand(HostCommand.EXECUTE_UPDATE);
    fixtures.host.enqueueCommand(HostCommand.SERVE_DATA);

    Thread thread = createPartitionServerThread(partitionServer);

    thread.start();
    Thread.sleep(2000);
    assertEquals(HostState.SERVING, fixtures.host.getState());

    partitionServer.onCommandQueueChange(fixtures.host);
    assertEquals(HostState.IDLE, fixtures.host.getState());

    partitionServer.onCommandQueueChange(fixtures.host);
    assertEquals(HostState.UPDATING, fixtures.host.getState());

    Thread.sleep(1500);
    assertTrue("Update called", updateManager.updateCalled);

    assertEquals(HostState.IDLE, fixtures.host.getState());

    partitionServer.onCommandQueueChange(fixtures.host);
    assertEquals(HostState.SERVING, fixtures.host.getState());

    assertNull("Current command cleared", fixtures.host.getCurrentCommand());

    partitionServer.stop();

    thread.join();
    assertEquals(HostState.OFFLINE, fixtures.host.getState());
  }

  public void testUpdateFailure() throws Exception {
    final FailingUpdateManager updateManager = new FailingUpdateManager();
    final PartitionServer partitionServer = new MockPartitionServer(fixtures.CONFIGURATOR1, "localhost") {
      @Override
      protected IUpdateManager getUpdateManager() {
        return updateManager;
      }
    };

    Thread thread = createPartitionServerThread(partitionServer);

    thread.start();
    Thread.sleep(1000);
    assertEquals(HostState.IDLE, fixtures.host.getState());
    fixtures.host.enqueueCommand(HostCommand.EXECUTE_UPDATE);
    partitionServer.onCommandQueueChange(fixtures.host);
    assertEquals(HostState.UPDATING, fixtures.host.getState());
    Thread.sleep(1500);
    assertEquals(HostState.IDLE, fixtures.host.getState());
    assertTrue("Update failed", updateManager.updateFailed);
    assertNull("Current command cleared", fixtures.host.getCurrentCommand());
    Thread.sleep(1500);
    assertEquals("Still IDLE after failed update.", HostState.IDLE, fixtures.host.getState());
  }

  public void testHostSetStateFailure() throws Exception {
    final PartitionServer partitionServer = new MockPartitionServer(fixtures.CONFIGURATOR2, "localhost");
    Thread thread = createPartitionServerThread(partitionServer);
    thread.start();
    Thread.sleep(1000);
    assertEquals(HostState.IDLE, fixtures.failingSetStateHost.getState());
    fixtures.failingSetStateHost.enqueueCommand(HostCommand.SERVE_DATA);
    partitionServer.onCommandQueueChange(fixtures.failingSetStateHost);
    Thread.sleep(1500);
    assertEquals("Went OFFLINE after failed state update.", HostState.OFFLINE, fixtures.failingSetStateHost.getState());
  }

  public void testHostNextCommandFailure() throws Exception {
    final PartitionServer partitionServer = new MockPartitionServer(fixtures.CONFIGURATOR3, "localhost");
    Thread thread = createPartitionServerThread(partitionServer);
    thread.start();
    Thread.sleep(1000);
    assertEquals(HostState.IDLE, fixtures.failingNextCommandHost.getState());
    fixtures.failingNextCommandHost.enqueueCommand(HostCommand.SERVE_DATA);
    partitionServer.onCommandQueueChange(fixtures.failingNextCommandHost);
    Thread.sleep(1500);
    assertEquals("Went OFFLINE after failed next command.", HostState.OFFLINE, fixtures.failingNextCommandHost.getState());
  }

  // Create a runnable thread that runs the given partition server
  public Thread createPartitionServerThread(final PartitionServer partitionServer) {
    Runnable serverRunnable = new Runnable() {
      @Override
      public void run() {
        try {
          partitionServer.run();
        } catch (Exception e) {
          fail("Exception! " + e);
        }
      }
    };
    return new Thread(serverRunnable, "PartitionServer thread");
  }
}
