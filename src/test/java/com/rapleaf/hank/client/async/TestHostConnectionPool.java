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
import com.rapleaf.hank.partition_server.IfaceWithShutdown;
import com.rapleaf.hank.util.Condition;
import com.rapleaf.hank.util.WaitUntil;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHostConnectionPool extends BaseTestCase {

  private static final Logger LOG = Logger.getLogger(TestHostConnectionPool.class);

  private static final ByteBuffer KEY_1 = ByteBuffer.wrap("1".getBytes());
  private static final HankResponse RESPONSE_1 = HankResponse.value(KEY_1);

  private static final PartitionServerAddress partitionServerAddress1 = new PartitionServerAddress("localhost", 50004);
  private static final PartitionServerAddress partitionServerAddress2 = new PartitionServerAddress("localhost", 50005);

  private final Host mockHost1 = new MockHost(partitionServerAddress1);
  private final Host mockHost2 = new MockHost(partitionServerAddress2);

  private Thread mockPartitionServerThread1;
  private Thread mockPartitionServerThread2;

  private TestHostConnection.MockPartitionServer mockPartitionServer1;
  private TestHostConnection.MockPartitionServer mockPartitionServer2;

  private static abstract class MockIface implements IfaceWithShutdown {

    private int numGets = 0;
    private int numGetBulks = 0;

    public void clearCounts() {
      numGets = 0;
      numGetBulks = 0;
    }

    protected abstract HankResponse getCore(int domain_id, ByteBuffer key) throws TException;

    protected abstract HankBulkResponse getBulkCore(int domain_id, List<ByteBuffer> keys) throws TException;

    @Override
    public void shutDown() throws InterruptedException {
    }

    @Override
    public HankResponse get(int domain_id, ByteBuffer key) throws TException {
      ++numGets;
      return getCore(domain_id, key);
    }

    @Override
    public HankBulkResponse getBulk(int domain_id, List<ByteBuffer> keys) throws TException {
      ++numGetBulks;
      return getBulkCore(domain_id, keys);
    }
  }

  private static class Response1Iface extends MockIface {

    @Override
    public HankResponse getCore(int domain_id, ByteBuffer key) throws TException {
      return RESPONSE_1;
    }

    @Override
    public HankBulkResponse getBulkCore(int domain_id, List<ByteBuffer> keys) throws TException {
      return null;
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    mockHost1.setState(HostState.OFFLINE);
    mockHost2.setState(HostState.OFFLINE);
  }

  @Override
  public void tearDown() throws InterruptedException {
    stopPartitionServer(mockPartitionServer1, mockPartitionServerThread1);
    stopPartitionServer(mockPartitionServer2, mockPartitionServerThread2);
  }

  public void testHostConnectionSelection() throws IOException, TException, InterruptedException {

    MockIface iface1 = new Response1Iface();
    MockIface iface2 = new Response1Iface();
    TAsyncClientManager asyncClientManager = new TAsyncClientManager();
    Connector connector = new Connector();

    startMockPartitionServerThread1(iface1, 1);
    startMockPartitionServerThread2(iface2, 1);

    Map<Host, List<HostConnection>> hostToConnectionsMap = new HashMap<Host, List<HostConnection>>();

    int establishConnectionTimeoutMs = 0;
    int queryTimeoutMs = 0;
    int bulkQueryTimeoutMs = 0;
    HostConnection connection1 = new HostConnection(mockHost1,
        null,
        asyncClientManager,
        establishConnectionTimeoutMs,
        queryTimeoutMs,
        bulkQueryTimeoutMs);
    HostConnection connection2 = new HostConnection(mockHost2,
        null,
        asyncClientManager,
        establishConnectionTimeoutMs,
        queryTimeoutMs,
        bulkQueryTimeoutMs);
    hostToConnectionsMap.put(mockHost1, Collections.singletonList(connection1));
    hostToConnectionsMap.put(mockHost2, Collections.singletonList(connection2));

    HostConnectionPool hostConnectionPool = new HostConnectionPool(hostToConnectionsMap, connector, null);
    HashMap<HostConnection, Integer> counter = new HashMap<HostConnection, Integer>();

    mockHost1.setState(HostState.SERVING);
    mockHost2.setState(HostState.SERVING);

    HostConnectionPool.HostConnectionAndHostIndex connection = hostConnectionPool.findConnectionToUse();
    assertNull(connection);
    assertTrue(connection1.isConnecting());
    assertTrue(connection2.isConnecting());

    connection1.attemptConnect();
    connection2.attemptConnect();

    assertTrue(connection1.isConnected());
    assertTrue(connection2.isConnected());

    counter.put(connection1, 0);
    counter.put(connection2, 0);

    HostConnectionPool.HostConnectionAndHostIndex previousHostConnection = null;
    for (int i = 0; i < 10; ++i) {
      if (previousHostConnection == null) {
        previousHostConnection = hostConnectionPool.findConnectionToUse();
      } else {
        previousHostConnection = hostConnectionPool.findNextConnectionToUse(previousHostConnection.hostIndex);
      }
      HostConnection hostConnection = previousHostConnection.hostConnection;
      counter.put(hostConnection, counter.get(hostConnection) + 1);
    }
    assertTrue("Half of connection request should be on for this host", counter.get(connection1) == 5);
    assertTrue("Half of connection request should be on for this host", counter.get(connection2) == 5);

    counter.put(connection1, 0);
    counter.put(connection2, 0);

    connection1.setIsBusy(true);

    previousHostConnection = null;
    for (int i = 0; i < 10; ++i) {
      if (previousHostConnection == null) {
        previousHostConnection = hostConnectionPool.findConnectionToUse();
      } else {
        previousHostConnection = hostConnectionPool.findNextConnectionToUse(previousHostConnection.hostIndex);
      }
      HostConnection hostConnection = previousHostConnection.hostConnection;
      counter.put(hostConnection, counter.get(hostConnection) + 1);
    }
    assertTrue("No connection request should be on this host since currently used", counter.get(connection1) == 0);
    assertTrue("All of connection request should be on for this host", counter.get(connection2) == 10);
    connection1.setIsBusy(false);

    counter.put(connection1, 0);
    counter.put(connection2, 0);

    mockHost2.setState(HostState.UPDATING);
    previousHostConnection = null;
    for (int i = 0; i < 10; ++i) {
      if (previousHostConnection == null) {
        previousHostConnection = hostConnectionPool.findConnectionToUse();
      } else {
        previousHostConnection = hostConnectionPool.findNextConnectionToUse(previousHostConnection.hostIndex);
      }
      HostConnection hostConnection = previousHostConnection.hostConnection;
      counter.put(hostConnection, counter.get(hostConnection) + 1);
    }
    assertTrue("All of connection request should be on for this host", counter.get(connection1) == 10);
    assertTrue("No connection request should be on this host since UPDATING", counter.get(connection2) == 0);

    counter.put(connection1, 0);
    counter.put(connection2, 0);

    mockHost1.setState(HostState.UPDATING);
    mockHost2.setState(HostState.UPDATING);
    previousHostConnection = null;
    for (int i = 0; i < 10; ++i) {
      if (previousHostConnection == null) {
        previousHostConnection = hostConnectionPool.findConnectionToUse();
      } else {
        previousHostConnection = hostConnectionPool.findNextConnectionToUse(previousHostConnection.hostIndex);
      }
      if (previousHostConnection == null || previousHostConnection.hostConnection == null) {
        continue;
      }
      HostConnection hostConnection = previousHostConnection.hostConnection;
      counter.put(hostConnection, counter.get(hostConnection) + 1);
    }
    assertTrue("No connection request should be on this host since UPDATING", counter.get(connection1) == 0);
    assertTrue("No connection request should be on this host since UPDATING", counter.get(connection2) == 0);
  }

  public void testHostConnectionDeterministicShuffle() throws IOException, TException, InterruptedException {

    MockIface iface1 = new Response1Iface();
    MockIface iface2 = new Response1Iface();
    TAsyncClientManager asyncClientManager = new TAsyncClientManager();
    Connector connector = new Connector();

    startMockPartitionServerThread1(iface1, 1);
    startMockPartitionServerThread2(iface2, 1);

    Map<Host, List<HostConnection>> hostToConnectionsMap = new HashMap<Host, List<HostConnection>>();

    int establishConnectionTimeoutMs = 0;
    int queryTimeoutMs = 0;
    int bulkQueryTimeoutMs = 0;
    HostConnection connection1 = new HostConnection(mockHost1,
        null,
        asyncClientManager,
        establishConnectionTimeoutMs,
        queryTimeoutMs,
        bulkQueryTimeoutMs);
    HostConnection connection2 = new HostConnection(mockHost2,
        null,
        asyncClientManager,
        establishConnectionTimeoutMs,
        queryTimeoutMs,
        bulkQueryTimeoutMs);
    hostToConnectionsMap.put(mockHost1, Collections.singletonList(connection1));
    hostToConnectionsMap.put(mockHost2, Collections.singletonList(connection2));

    mockHost1.setState(HostState.SERVING);
    mockHost2.setState(HostState.SERVING);

    new HostConnectionPool(hostToConnectionsMap, connector, null).findConnectionToUse();
    new HostConnectionPool(hostToConnectionsMap, connector, null).findConnectionToUse();

    assertTrue(connection1.isConnecting());
    assertTrue(connection2.isConnecting());

    connection1.attemptConnect();
    connection2.attemptConnect();

    assertTrue(connection1.isConnected());
    assertTrue(connection2.isConnected());

    for (int n = 0; n < 1024; ++n) {
      HostConnectionPool hostConnectionPoolA = new HostConnectionPool(hostToConnectionsMap, connector, n);
      HostConnectionPool hostConnectionPoolB = new HostConnectionPool(hostToConnectionsMap, connector, n);
      for (int i = 0; i < 10; ++i) {
        HostConnectionPool.HostConnectionAndHostIndex connectionA = hostConnectionPoolA.findConnectionToUseForKey(n);
        HostConnectionPool.HostConnectionAndHostIndex connectionB = hostConnectionPoolB.findConnectionToUseForKey(n);
        assertEquals("Both connection pools attempt to use the same connection",
            connectionA.hostConnection.getHost(),
            connectionB.hostConnection.getHost());
      }
    }
  }

  private static void stopPartitionServer(TestHostConnection.MockPartitionServer mockPartitionServer, Thread mockPartitionServerThread) throws InterruptedException {
    if (mockPartitionServer != null) {
      LOG.info("Stopping partition server...");
      mockPartitionServer.stop();
    }
    if (mockPartitionServerThread != null) {
      mockPartitionServerThread.join();
      LOG.info("Stopped partition server");
    }
  }

  private void startMockPartitionServerThread1(IfaceWithShutdown handler, int numWorkerThreads)
      throws InterruptedException {
    mockPartitionServer1 = new TestHostConnection.MockPartitionServer(handler, numWorkerThreads,
        partitionServerAddress1);
    mockPartitionServerThread1 = new Thread(mockPartitionServer1);
    mockPartitionServerThread1.start();
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return mockPartitionServer1.dataServer != null
            && mockPartitionServer1.dataServer.isServing();
      }
    });
  }

  private void startMockPartitionServerThread2(IfaceWithShutdown handler, int numWorkerThreads)
      throws InterruptedException {
    mockPartitionServer2 = new TestHostConnection.MockPartitionServer(handler, numWorkerThreads,
        partitionServerAddress2);
    mockPartitionServerThread2 = new Thread(mockPartitionServer2);
    mockPartitionServerThread2.start();
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return mockPartitionServer2.dataServer != null
            && mockPartitionServer2.dataServer.isServing();
      }
    });
  }
}
