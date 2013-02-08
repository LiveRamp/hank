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

package com.rapleaf.hank.client.async;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.MockHost;
import com.rapleaf.hank.coordinator.PartitionServerAddress;
import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.PartitionServer;
import com.rapleaf.hank.partition_server.IfaceWithShutdown;
import com.rapleaf.hank.performance.HankTimer;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

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

  public void connect(HostConnection connection) throws IOException {
    if (connection.isDisconnected()) {
      connection.setConnecting();
    }
    connection.attemptConnect();
  }

  public void asyncGet(HostConnection connection, int domainId, ByteBuffer key) throws Exception {
    AsyncGetCallback callback = new AsyncGetCallback();
    connection.setIsBusy(true);
    connection.get(domainId, key, callback);
    try {
      callback.countDownLatch.await();
      if (callback.exception != null) {
        throw callback.exception;
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      connection.setIsBusy(false);
    }
  }

  public void asyncGetBulk(HostConnection connection, int domainId, List<ByteBuffer> keys) throws Exception {
    AsyncGetBulkCallback callback = new AsyncGetBulkCallback();
    connection.setIsBusy(true);
    connection.getBulk(domainId, keys, callback);
    try {
      callback.countDownLatch.await();
      if (callback.exception != null) {
        throw callback.exception;
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      connection.setIsBusy(false);
    }
  }

  public void testQueryOnlyServingHosts() throws Exception, TException, InterruptedException {

    int establishConnectionTimeoutMs = 0;
    int queryTimeoutMs = 100;
    int bulkQueryTimeoutMs = 100;

    HostConnection connection = new HostConnection(mockHost,
        null,
        new TAsyncClientManager(),
        establishConnectionTimeoutMs,
        queryTimeoutMs,
        bulkQueryTimeoutMs);

    // Should not query a non serving host
    mockHost.setState(HostState.IDLE);
    connect(connection);
    try {
      asyncGet(connection, 0, KEY_1);
      fail("Should fail");
    } catch (HostConnection.IllegalStateException e) {
      // If we don't catch the exception then the test will fail
    }
    try {
      asyncGetBulk(connection, 0, Collections.singletonList(KEY_1));
      fail("Should fail");
    } catch (HostConnection.IllegalStateException e) {
      // If we don't catch the exception then the test will fail
    }

    // Should succeed quering a serving host
    mockHost.setState(HostState.SERVING);
    startMockPartitionServerThread(mockIface, 1);
    connect(connection);
    asyncGet(connection, 0, KEY_1);
    asyncGetBulk(connection, 0, Collections.singletonList(KEY_1));
  }

  public void testConnectionTimeout() throws IOException, TException {
    int establishConnectionTimeoutMs = 100;
    int queryTimeoutMs = 100;
    int bulkQueryTimeoutMs = 100;

    try {
      new HostConnection(mockHost,
          null,
          new TAsyncClientManager(),
          establishConnectionTimeoutMs,
          queryTimeoutMs,
          bulkQueryTimeoutMs);

      fail("Should fail");
    } catch (NotImplementedException e) {
      // If we don't catch the exception then the test will fail
    }
  }

  class NotifierMock implements Runnable {
    public boolean notified = false;

    @Override
    public void run() {
      notified = true;
    }
  }

  public void testListenerWakeup() throws IOException, TException {
    int queryTimeoutMs = 10;
    int bulkQueryTimeoutMs = 10;
    NotifierMock notifier = new NotifierMock();

    HostConnection connection = new HostConnection(mockHost,
        notifier,
        new TAsyncClientManager(),
        0,
        queryTimeoutMs,
        bulkQueryTimeoutMs);
    mockHost.setState(HostState.SERVING);
    connect(connection);

    assertTrue("Notifier should have been notified.", notifier.notified);
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

    HostConnection connection = new HostConnection(mockHost,
        null,
        new TAsyncClientManager(),
        0,
        100,
        100);
    mockHost.setState(HostState.SERVING);

    // Test GET Timeout
    connect(connection);
    timer.restart();
    try {
      asyncGet(connection, 0, KEY_1);
      fail("Should fail");
    } catch (Exception e) {
      duration = timer.getDuration() / 1000000l;
      // Check that correct exception was raised
      assertTrue(e instanceof TimeoutException);
    }
    // Check that timeout was respected
    LOG.info("Took " + duration + "ms");
    // 10ms get timeout and 10ms for the rest
    assertTrue(duration < 100 + 10);


    // Test GET BULK Timeout
    connection.attemptDisconnect();
    connect(connection);
    timer.restart();
    try {
      asyncGetBulk(connection, 0, Collections.singletonList(KEY_1));
      fail("Should fail");
    } catch (Exception e) {
      duration = timer.getDuration() / 1000000l;
      // Check that correct exception was raised
      assertTrue(e instanceof TimeoutException);
    }
    // Check that timeout was respected
    LOG.info("Took " + duration + "ms");
    // 10ms get timeout and 10ms for the rest
    assertTrue(duration < 100 + 10);

    connection.attemptDisconnect();
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

  private static class AsyncGetCallback implements HostConnectionGetCallback {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    Exception exception;

    @Override
    public void onComplete(PartitionServer.AsyncClient.get_call response) {
      countDownLatch.countDown();
    }

    @Override
    public void onError(Exception e) {
      exception = e;
      countDownLatch.countDown();
    }
  }

  private static class AsyncGetBulkCallback implements HostConnectionGetBulkCallback {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    Exception exception;

    @Override
    public void onComplete(PartitionServer.AsyncClient.getBulk_call response) {
      countDownLatch.countDown();
    }

    @Override
    public void onError(Exception e) {
      exception = e;
      countDownLatch.countDown();
    }
  }

  private void startMockPartitionServerThread(IfaceWithShutdown handler, int numWorkerThreads)
      throws InterruptedException {
    mockPartitionServer = new MockPartitionServer(handler, numWorkerThreads, partitionServerAddress);
    mockPartitionServerThread = new Thread(mockPartitionServer);
    mockPartitionServerThread.start();
    while (mockPartitionServer.dataServer == null ||
        !mockPartitionServer.dataServer.isServing()) {
      LOG.info("Waiting for data server to start serving...");
      Thread.sleep(100);
    }
  }
/*
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
*/
}
