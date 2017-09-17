/**
 *  Copyright 2011 LiveRamp
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
package com.liveramp.hank.partition_server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Before;
import org.junit.Test;

import com.liveramp.hank.config.PartitionServerConfigurator;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostCommand;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.coordinator.mock.MockCoordinator;
import com.liveramp.hank.generated.HankBulkResponse;
import com.liveramp.hank.generated.HankResponse;
import com.liveramp.hank.test.BaseTestCase;
import com.liveramp.hank.test.coordinator.MockHost;
import com.liveramp.hank.test.coordinator.MockRing;
import com.liveramp.hank.test.coordinator.MockRingGroup;
import com.liveramp.hank.util.Condition;
import com.liveramp.hank.util.WaitUntil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestPartitionServer extends BaseTestCase {

  private Fixtures fixtures;

  @Before
  public void setUp() throws Exception {
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
        throw new IOException("Failure to move on to next command.");
      }
    };

    final Ring mockRing = new MockRing(null, null, 0) {
      @Override
      public Host getHostByAddress(PartitionServerAddress address) {
        switch (address.getPortNumber()) {
          case PORT_1:
            // Mock Host
            return host;
          case PORT_2:
            // Failing setState() Host
            return failingSetStateHost;
          case PORT_3:
            // Failing nextCommand() Host
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
          return HankResponse.not_found(true);
        }

        @Override
        public HankBulkResponse getBulk(int domainId, List<ByteBuffer> keys) throws TException {
          return HankBulkResponse.responses(Collections.singletonList(HankResponse.not_found(true)));
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

  @Test
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
    waitUntilHost(HostState.IDLE, fixtures.host);
    assertEquals(HostState.IDLE, fixtures.host.getState());

    fixtures.host.enqueueCommand(HostCommand.SERVE_DATA);
    waitUntilHost(HostState.SERVING, fixtures.host);
    assertEquals(HostState.SERVING, fixtures.host.getState());

    fixtures.host.enqueueCommand(HostCommand.GO_TO_IDLE);
    waitUntilHost(HostState.IDLE, fixtures.host);
    assertEquals(HostState.IDLE, fixtures.host.getState());

    fixtures.host.enqueueCommand(HostCommand.EXECUTE_UPDATE);
    waitUntilHost(HostState.UPDATING, fixtures.host);
    assertEquals(HostState.UPDATING, fixtures.host.getState());

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return updateManager.updateCalled
              && fixtures.host.getCurrentCommand() == null
              && HostState.IDLE.equals(fixtures.host.getState());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    assertTrue("Update called", updateManager.updateCalled);
    assertNull("Current command cleared", fixtures.host.getCurrentCommand());
    assertEquals(HostState.IDLE, fixtures.host.getState());

    partitionServer.stopSynchronized();

    thread.join();
    assertEquals(HostState.OFFLINE, fixtures.host.getState());
  }


  @Test
  public void testWaitUntilRingAssignment() throws IOException, InterruptedException {

    final Ring mockRing = new MockRing(null, null, 0);

    final Map<PartitionServerAddress, Ring> hostToRing = Maps.newHashMap();

    final RingGroup mockRingGroup = new MockRingGroup(null, "myRingGroup", null) {
      @Override
      public Ring getRingForHost(PartitionServerAddress hostAddress) {
        return hostToRing.get(hostAddress);
      }
    };

    final MockCoordinator mockCoord = new MockCoordinator() {
      @Override
      public RingGroup getRingGroup(String ringGroupName) {
        return mockRingGroup;
      }
    };

    MockPartitionServerConfigurator configurator = new MockPartitionServerConfigurator(12345, mockCoord, "myRingGroup", null);

    final PartitionServer partitionServer = new MockPartitionServer(configurator, "localhost");

    AtomicBoolean serverDead = new AtomicBoolean(false);
    new Thread(() -> {
      try {
        partitionServer.run();
        serverDead.set(true);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).start();

    //  host should have registered
    WaitUntil.orDie(() -> {
      try {
        return mockRingGroup.getLiveServers().size() == 1;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    //  and not died
    assertFalse(serverDead.get());

    PartitionServerAddress address = new PartitionServerAddress("localhost", 12345);
    mockRing.addHost(address, Lists.newArrayList());
    hostToRing.put(address, mockRing);

    WaitUntil.orDie(() -> {
      try {
        return mockRing.getHostByAddress(address).getState() == HostState.IDLE;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

  }

  @Test
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

    waitUntilHost(HostState.SERVING, fixtures.host);

    assertTrue("Update was called", updateManager.updateCalled);

    assertEquals(HostState.SERVING, fixtures.host.getState());

    assertNull("Current command cleared", fixtures.host.getCurrentCommand());

    partitionServer.stopSynchronized();

    thread.join();
    assertEquals(HostState.OFFLINE, fixtures.host.getState());
  }

  @Test
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
    waitUntilHost(HostState.IDLE, fixtures.host);
    assertEquals(HostState.IDLE, fixtures.host.getState());
    fixtures.host.enqueueCommand(HostCommand.EXECUTE_UPDATE);
    waitUntilHost(HostState.UPDATING, fixtures.host);
    assertEquals(HostState.UPDATING, fixtures.host.getState());
    waitUntilHost(HostState.IDLE, fixtures.host);
    assertEquals(HostState.IDLE, fixtures.host.getState());
    assertTrue("Update failed", updateManager.updateFailed);
    assertNull("Current command cleared", fixtures.host.getCurrentCommand());
    waitUntilHost(HostState.IDLE, fixtures.host);
    assertEquals("Still IDLE after failed update.", HostState.IDLE, fixtures.host.getState());
  }

  @Test
  public void testHostSetStateFailure() throws Exception {
    final   PartitionServer partitionServer = new MockPartitionServer(fixtures.CONFIGURATOR2, "localhost");
    Thread thread = createPartitionServerThread(partitionServer);
    thread.start();
    waitUntilHost(HostState.IDLE, fixtures.failingSetStateHost);
    assertEquals(HostState.IDLE, fixtures.failingSetStateHost.getState());
    fixtures.failingSetStateHost.enqueueCommand(HostCommand.SERVE_DATA);
    thread.join();
    assertEquals("Went OFFLINE after failed state update.", HostState.OFFLINE, fixtures.failingSetStateHost.getState());
  }

  @Test
  public void testHostNextCommandFailure() throws Exception {
    final PartitionServer partitionServer = new MockPartitionServer(fixtures.CONFIGURATOR3, "localhost");
    Thread thread = createPartitionServerThread(partitionServer);
    thread.start();
    thread.join();
    assertEquals(HostState.OFFLINE, fixtures.failingNextCommandHost.getState());
    assertEquals("Went OFFLINE after failed next command.", HostState.OFFLINE, fixtures.failingNextCommandHost.getState());
  }

  @Test
  public void testFailingThriftDataServer() throws Exception {
    final PartitionServer partitionServer = new MockPartitionServer(fixtures.CONFIGURATOR1, "localhost") {
      @Override
      protected void startThriftServer() throws TTransportException, IOException, InterruptedException {
        throw new RuntimeException("Failed to start Thrift server.");
      }
    };
    fixtures.host.enqueueCommand(HostCommand.SERVE_DATA);
    Thread thread = createPartitionServerThread(partitionServer);
    thread.start();
    thread.join();
    assertEquals(HostState.OFFLINE, fixtures.host.getState());
    assertEquals("Went OFFLINE after failed to start data server.", HostState.OFFLINE, fixtures.host.getState());
  }

  @Test
  public void testFailToStartWhenHostIsAlreadyOnline() throws IOException, InterruptedException {
    final PartitionServer partitionServer = new MockPartitionServer(fixtures.CONFIGURATOR1, "localhost");
    Thread thread = createPartitionServerThread(partitionServer);
    thread.start();
    Thread.sleep(500);
    try {
      new MockPartitionServer(fixtures.CONFIGURATOR1, "localhost").run();
      fail("Should fail to start when host is already online.");
    } catch (Exception e) {
    }
    partitionServer.stopSynchronized();
    thread.join();
  }


  // Create a runnable thread that runs the given partition server
  public Thread createPartitionServerThread(final PartitionServer partitionServer) {
    Runnable serverRunnable = new Runnable() {
      @Override
      public void run() {
        try {
          partitionServer.run();
        } catch (Exception e) {
          e.printStackTrace();
          fail("Exception!");
        }
      }
    };
    return new Thread(serverRunnable, "PartitionServer thread");
  }
}
