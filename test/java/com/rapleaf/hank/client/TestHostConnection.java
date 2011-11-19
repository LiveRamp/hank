/*
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

package com.rapleaf.hank.client;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.MockHost;
import com.rapleaf.hank.coordinator.PartitionServerAddress;
import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.partition_server.IfaceWithShutdown;
import com.rapleaf.hank.performance.HankTimer;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class TestHostConnection extends BaseTestCase {

  private static final Logger LOG = Logger.getLogger(TestHostConnection.class);

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
    public HankResponse get(int domain_id, ByteBuffer key) throws TException {
      return RESPONSE_1;
    }

    @Override
    public HankBulkResponse getBulk(int domain_id, List<ByteBuffer> keys) throws TException {
      return RESPONSE_BULK_1;
    }
  };

  private Thread mockPartitionServerThread;
  private MockPartitionServer mockPartitionServer;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    mockHost.setState(HostState.OFFLINE);
  }

  @Override
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

  private class MockPartitionServer implements Runnable {

    private final IfaceWithShutdown handler;
    private final int numWorkerThreads;
    private TServer dataServer;

    MockPartitionServer(IfaceWithShutdown handler, int numWorkerThreads) {
      this.handler = handler;
      this.numWorkerThreads = numWorkerThreads;
    }

    @Override
    public void run() {
      // launch the thrift server
      TNonblockingServerSocket serverSocket = null;
      try {
        serverSocket = new TNonblockingServerSocket(partitionServerAddress.getPortNumber());
      } catch (TTransportException e) {
        throw new RuntimeException(e);
      }
      THsHaServer.Args options = new THsHaServer.Args(serverSocket);
      options.processor(new com.rapleaf.hank.generated.PartitionServer.Processor(handler));
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
      dataServer.stop();
    }
  }

  private void startMockPartitionServerThread(IfaceWithShutdown handler, int numWorkerThreads)
      throws InterruptedException {
    mockPartitionServer = new MockPartitionServer(handler, numWorkerThreads);
    mockPartitionServerThread = new Thread(mockPartitionServer);
    mockPartitionServerThread.start();
    while (mockPartitionServer.dataServer == null ||
        !mockPartitionServer.dataServer.isServing()) {
      LOG.info("Waiting for data server to start serving...");
      Thread.sleep(100);
    }
  }

  public void testQueryOnlyServingHosts() throws IOException, TException, InterruptedException {

    int tryLockTimeoutMs = 1000;
    int establishConnectionTimeoutMs = 1000;
    int queryTimeoutMs = 10;
    int bulkQueryTimeoutMs = 10;

    HostConnection connection = new HostConnection(mockHost,
        tryLockTimeoutMs,
        establishConnectionTimeoutMs,
        queryTimeoutMs,
        bulkQueryTimeoutMs);

    // Should not query a non serving host
    mockHost.setState(HostState.IDLE);
    try {
      connection.get(0, KEY_1);
    } catch (Exception e) {
      assertEquals("Connection to host is not available (host is not serving).", e.getMessage());
    }
    try {
      connection.getBulk(0, Collections.singletonList(KEY_1));
    } catch (Exception e) {
      assertEquals("Connection to host is not available (host is not serving).", e.getMessage());
    }

    // Should succeed quering a serving host
    mockHost.setState(HostState.SERVING);
    startMockPartitionServerThread(mockIface, 1);
    assertEquals(RESPONSE_1, connection.get(0, KEY_1));
    assertEquals(RESPONSE_BULK_1, connection.getBulk(0, Collections.singletonList(KEY_1)));
  }

  public void testGetTimeouts() throws IOException, TException, InterruptedException {

    mockHost.setState(HostState.SERVING);

    IfaceWithShutdown hangingIface = new IfaceWithShutdown() {
      @Override
      public void shutDown() throws InterruptedException {
      }

      @Override
      public HankResponse get(int domain_id, ByteBuffer key) throws TException {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return null;
      }

      @Override
      public HankBulkResponse getBulk(int domain_id, List<ByteBuffer> keys) throws TException {
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
    // 10ms get timeout and 10ms for the rest
    assertTrue(duration < 100 + 10);


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
    // 10ms get timeout and 10ms for the rest
    assertTrue(duration < 100 + 10);
  }

  public void testTryLockTimeout() throws IOException, TException, InterruptedException {

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
    while (!connection.lock.isLocked() ||
        connection.lock.isHeldByCurrentThread()) {
      LOG.info("Waiting for connection lock to be locked by another thread.");
      Thread.sleep(100);
    }

    // Try to perform a get
    HankTimer timer = new HankTimer();
    try {
      connection.get(0, KEY_1);
      fail("Should fail");
    } catch (IOException e) {
      long duration = timer.getDuration() / 1000000l;
      assertEquals("Exceeded timeout while trying to lock the host connection.", e.getMessage());
      assertTrue(duration < 100 + 10);
    } finally {
      // Kill the locking thread
      lockingThread.interrupt();
    }
  }

  private HankResponse get(int tryLockTimeoutMs,
                           int establishConnectionTimeoutMs,
                           int queryTimeoutMs,
                           int bulkQueryTimeoutMs) throws IOException, TException {
    return new HostConnection(mockHost, tryLockTimeoutMs, establishConnectionTimeoutMs,
        queryTimeoutMs, bulkQueryTimeoutMs).get(0, KEY_1);
  }

  private HankBulkResponse getBulk(int tryLockTimeoutMs,
                                   int establishConnectionTimeoutMs,
                                   int queryTimeoutMs,
                                   int bulkQueryTimeoutMs) throws IOException, TException {
    return new HostConnection(mockHost, tryLockTimeoutMs, establishConnectionTimeoutMs,
        queryTimeoutMs, bulkQueryTimeoutMs).getBulk(0, Collections.singletonList(KEY_1));
  }
}
