/*
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

package com.liveramp.hank.client;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.generated.HankBulkResponse;
import com.liveramp.hank.generated.HankResponse;
import com.liveramp.hank.partition_server.IfaceWithShutdown;
import com.liveramp.hank.test.BaseTestCase;
import com.liveramp.hank.test.coordinator.MockHost;
import com.liveramp.hank.util.Condition;
import com.liveramp.hank.util.HankTimer;
import com.liveramp.hank.util.WaitUntil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestHostConnection extends BaseTestCase {

  private static final Logger LOG = LoggerFactory.getLogger(TestHostConnection.class);

  private static final PartitionServerAddress partitionServerAddress = new PartitionServerAddress("localhost", 50004);

  private final Host mockHost = new MockHost(partitionServerAddress);

  private static final ByteBuffer KEY_1 = ByteBuffer.wrap("1".getBytes());

  private static final HankResponse RESPONSE_1 = HankResponse.value(KEY_1);
  private static final HankBulkResponse RESPONSE_BULK_1 = HankBulkResponse.responses(Collections.singletonList(HankResponse.value(KEY_1)));
  private static final IfaceWithShutdown mockIface = new IfaceWithShutdown() {
    @Override
    public void shutDown() throws InterruptedException {
    }

    @Override
    public HankResponse get(int domain_id, ByteBuffer key) {
      return RESPONSE_1;
    }

    @Override
    public HankBulkResponse getBulk(int domain_id, List<ByteBuffer> keys) {
      return RESPONSE_BULK_1;
    }
  };

  private Thread mockPartitionServerThread;
  private MockPartitionServer mockPartitionServer;

  @Before
  public void setUp() throws Exception {
    mockHost.setState(HostState.OFFLINE);
  }

  @After
  public void tearDown() throws InterruptedException {
    if (mockPartitionServer != null) {
      LOG.info("Stopping partition server...");
      mockPartitionServer.stop();
    }
    if (mockPartitionServerThread != null) {
      mockPartitionServerThread.join();
      LOG.info("Stopped partition server");
    }
  }

  @Test
  public void testQueryOnlyServingHosts() throws IOException, InterruptedException {

    int tryLockTimeoutMs = 1000;
    int establishConnectionTimeoutMs = 1000;
    int queryTimeoutMs = 1000;
    int bulkQueryTimeoutMs = 1000;

    HostConnection connection = new HostConnection(mockHost,
        tryLockTimeoutMs,
        establishConnectionTimeoutMs,
        queryTimeoutMs,
        bulkQueryTimeoutMs);

    // Should not query an idle host
    mockHost.setState(HostState.IDLE);
    try {
      connection.get(0, KEY_1);
      fail("Should fail");
    } catch (Exception e) {
      assertEquals("Connection to host is not available (host is not serving).", e.getMessage());
    }
    try {
      connection.getBulk(0, Collections.singletonList(KEY_1));
      fail("Should fail");
    } catch (Exception e) {
      assertEquals("Connection to host is not available (host is not serving).", e.getMessage());
    }

    // Should succeed quering a serving host
    mockHost.setState(HostState.SERVING);
    startMockPartitionServerThread(mockIface, 1);
    assertEquals(RESPONSE_1, connection.get(0, KEY_1));
    assertEquals(RESPONSE_BULK_1, connection.getBulk(0, Collections.singletonList(KEY_1)));

    // Should try to query an "offline" host only if that is the only option
    mockHost.setState(HostState.OFFLINE);
    assertEquals(RESPONSE_1, connection.get(0, KEY_1));
    assertEquals(RESPONSE_BULK_1, connection.getBulk(0, Collections.singletonList(KEY_1)));
  }

  @Test
  public void testGetTimeouts() throws IOException, InterruptedException {

    mockHost.setState(HostState.SERVING);

    IfaceWithShutdown hangingIface = new IfaceWithShutdown() {
      @Override
      public void shutDown() throws InterruptedException {
      }

      @Override
      public HankResponse get(int domain_id, ByteBuffer key) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return null;
      }

      @Override
      public HankBulkResponse getBulk(int domain_id, List<ByteBuffer> keys) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return null;
      }
    };

    // Start server
    startMockPartitionServerThread(hangingIface, 1);

    long duration = Long.MAX_VALUE;
    HankTimer timer = new HankTimer();

    // Test GET Timeout
    timer.restart();
    try {
      get(1000, 1000, 100, 1000);
      fail("Should fail");
    } catch (IOException e) {
      duration = timer.getDuration() / 1000000l;
      // Check that correct exception was raised
      assertTrue(e.getCause().getCause() instanceof SocketTimeoutException);
    }
    // Check that timeout was respected
    LOG.info("Took " + duration + "ms");
    assertTrue(duration < 1000);


    // Test GET BULK Timeout
    timer.restart();
    try {
      getBulk(1000, 1000, 1000, 100);
      fail("Should fail");
    } catch (IOException e) {
      duration = timer.getDuration() / 1000000l;
      // Check that correct exception was raised
      assertTrue(e.getCause().getCause() instanceof SocketTimeoutException);
    }
    // Check that timeout was respected
    LOG.info("Took " + duration + "ms");
    assertTrue(duration < 1000);
  }

  @Test
  public void testTryLockTimeout() throws IOException, InterruptedException {

    // Start server
    startMockPartitionServerThread(mockIface, 1);

    final HostConnection connection = new HostConnection(mockHost, 100, 1000, 1000, 1000);

    Thread lockingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        connection.lock.lock();
        while (true) {
          try {
            // Keep lock until interrupted
            LOG.info("Holding lock until interrupted...");
            Thread.sleep(100);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    });

    // Lock the connection
    lockingThread.start();

    // Wait for the connection to be locked
    WaitUntil.condition(new Condition() {
      @Override
      public boolean test() {
        return connection.lock.isLocked() &&
            !connection.lock.isHeldByCurrentThread();
      }
    });

    // Try to perform a get
    try {
      connection.get(0, KEY_1);
      fail("Should fail");
    } catch (IOException e) {
      assertEquals("Exceeded timeout while trying to lock the host connection.", e.getMessage());
    } finally {
      // Kill the locking thread
      lockingThread.interrupt();
    }
  }

  public static class MockPartitionServer implements Runnable {

    private final IfaceWithShutdown handler;
    private final int numWorkerThreads;
    private final PartitionServerAddress partitionServerAddress;
    protected TServer dataServer;

    MockPartitionServer(IfaceWithShutdown handler, int numWorkerThreads, PartitionServerAddress partitionServerAddress) {
      this.handler = handler;
      this.numWorkerThreads = numWorkerThreads;
      this.partitionServerAddress = partitionServerAddress;
    }

    @Override
    public void run() {
      // launch the thrift server
      TNonblockingServerSocket serverSocket;
      try {
        serverSocket = new TNonblockingServerSocket(partitionServerAddress.getPortNumber());
      } catch (TTransportException e) {
        throw new RuntimeException(e);
      }
      THsHaServer.Args options = new THsHaServer.Args(serverSocket);
      options.processor(new com.liveramp.hank.generated.PartitionServer.Processor(handler));
      options.workerThreads(numWorkerThreads);
      options.protocolFactory(new TCompactProtocol.Factory());
      dataServer = new THsHaServer(options);
      LOG.debug("Launching Thrift server...");
      dataServer.serve();
      LOG.debug("Thrift server exited.");
      try {
        handler.shutDown();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      LOG.debug("Handler shutdown.");
    }

    public void stop() {
      if (dataServer != null) {
        dataServer.stop();
      }
    }
  }

  private void startMockPartitionServerThread(IfaceWithShutdown handler, int numWorkerThreads)
      throws InterruptedException {
    mockPartitionServer = new MockPartitionServer(handler, numWorkerThreads, partitionServerAddress);
    mockPartitionServerThread = new Thread(mockPartitionServer);
    mockPartitionServerThread.start();
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return mockPartitionServer.dataServer != null &&
            mockPartitionServer.dataServer.isServing();
      }
    });
  }

  private HankResponse get(int tryLockTimeoutMs,
                           int establishConnectionTimeoutMs,
                           int queryTimeoutMs,
                           int bulkQueryTimeoutMs) throws IOException {
    return new HostConnection(mockHost, tryLockTimeoutMs, establishConnectionTimeoutMs,
        queryTimeoutMs, bulkQueryTimeoutMs).get(0, KEY_1);
  }

  private HankBulkResponse getBulk(int tryLockTimeoutMs,
                                   int establishConnectionTimeoutMs,
                                   int queryTimeoutMs,
                                   int bulkQueryTimeoutMs) throws IOException {
    return new HostConnection(mockHost, tryLockTimeoutMs, establishConnectionTimeoutMs,
        queryTimeoutMs, bulkQueryTimeoutMs).getBulk(0, Collections.singletonList(KEY_1));
  }
}
