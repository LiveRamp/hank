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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.mock.MockDomain;
import com.liveramp.hank.generated.HankBulkResponse;
import com.liveramp.hank.generated.HankException;
import com.liveramp.hank.generated.HankResponse;
import com.liveramp.hank.partition_server.IfaceWithShutdown;
import com.liveramp.hank.test.BaseTestCase;
import com.liveramp.hank.test.coordinator.MockHost;
import com.liveramp.hank.util.Condition;
import com.liveramp.hank.util.WaitUntil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class TestHostConnectionPool extends BaseTestCase {

  private static final Logger LOG = LoggerFactory.getLogger(TestHostConnectionPool.class);

  private static final ByteBuffer KEY_1 = ByteBuffer.wrap("1".getBytes());
  private static final HankResponse RESPONSE_1 = HankResponse.value(KEY_1);

  private static final PartitionServerAddress partitionServerAddress1 = new PartitionServerAddress("localhost", 50004);
  private static final PartitionServerAddress partitionServerAddress2 = new PartitionServerAddress("localhost", 50005);
  private static final PartitionServerAddress partitionServerAddress3 = new PartitionServerAddress("localhost", 50006);

  private Host mockHost1;
  private Host mockHost2;
  private Host mockHost3;

  private Thread mockPartitionServerThread1;
  private Thread mockPartitionServerThread2;
  private Thread mockPartitionServerThread3;

  private TestHostConnection.MockPartitionServer mockPartitionServer1;
  private TestHostConnection.MockPartitionServer mockPartitionServer2;
  private TestHostConnection.MockPartitionServer mockPartitionServer3;

  private Domain mockDomain = new MockDomain("domain");


  private static abstract class MockIface implements IfaceWithShutdown {

    private int numGets = 0;
    private int numCompletedGets = 0;

    public void clearCounts() {
      numGets = 0;
      numCompletedGets = 0;
    }

    protected abstract HankResponse getCore(int domain_id, ByteBuffer key);

    @Override
    public void shutDown() throws InterruptedException {
    }

    @Override
    public HankResponse get(int domain_id, ByteBuffer key) {
      ++numGets;
      HankResponse result = getCore(domain_id, key);
      ++numCompletedGets;
      return result;
    }

    @Override
    public HankBulkResponse getBulk(int domain_id, List<ByteBuffer> keys) {
      return null;
    }
  }

  private static class Response1Iface extends MockIface {

    @Override
    public HankResponse getCore(int domain_id, ByteBuffer key) {
      return RESPONSE_1;
    }
  }

  private class HangingIface extends MockIface {

    private final Semaphore semaphore;

    public HangingIface(Semaphore semaphore) {
      this.semaphore = semaphore;
    }

    @Override
    public HankResponse getCore(int domain_id, ByteBuffer key) {
      try {
        this.semaphore.acquire();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return null;
    }
  }

  private class HankExceptionIface extends MockIface {

    @Override
    protected HankResponse getCore(int domain_id, ByteBuffer key) {
      return HankResponse.xception(HankException.internal_error("Internal Error"));
    }
  }

  @Before
  public void setUp() throws Exception {

    mockHost1 = new MockHost(partitionServerAddress1);
    mockHost2 = new MockHost(partitionServerAddress2);
    mockHost3 = new MockHost(partitionServerAddress3);

    mockHost1.setState(HostState.OFFLINE);
    mockHost2.setState(HostState.OFFLINE);
    mockHost3.setState(HostState.OFFLINE);

  }

  @After
  public void tearDown() throws InterruptedException {
    stopPartitionServer(mockPartitionServer1, mockPartitionServerThread1);
    stopPartitionServer(mockPartitionServer2, mockPartitionServerThread2);
    stopPartitionServer(mockPartitionServer3, mockPartitionServerThread3);

  }

  @Test
  public void testBothUp() throws IOException, InterruptedException {

    MockIface iface1 = new Response1Iface();
    MockIface iface2 = new Response1Iface();

    startMockPartitionServerThread1(iface1, 1);
    startMockPartitionServerThread2(iface2, 1);

    Map<Host, List<HostConnection>> hostToConnectionsMap = new HashMap<Host, List<HostConnection>>();

    int tryLockTimeoutMs = 0;
    int establishConnectionTimeoutMs = 0;
    int queryTimeoutMs = 0;
    int bulkQueryTimeoutMs = 0;

    hostToConnectionsMap.put(mockHost1, Collections.singletonList(new HostConnection(mockHost1,
        tryLockTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs)));
    hostToConnectionsMap.put(mockHost2, Collections.singletonList(new HostConnection(mockHost2,
        tryLockTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs)));

    HostConnectionPool hostConnectionPool = new HostConnectionPool(hostToConnectionsMap, null, Sets.newHashSet());

    mockHost1.setState(HostState.SERVING);
    mockHost2.setState(HostState.SERVING);

    int numHits = 0;

    for (int i = 0; i < 10; ++i) {
      HankResponse response = hostConnectionPool.get(mockDomain, KEY_1, 1, null);
      assertEquals(RESPONSE_1, response);
      if (response.is_set_value()) {
        ++numHits;
      }
    }
    assertEquals("Gets should be distributed accross hosts", 5, iface1.numGets);
    assertEquals("Gets should be distributed accross hosts", 5, iface2.numGets);
    assertEquals("All keys should have been found", 10, numHits);

    iface1.clearCounts();
    iface2.clearCounts();

    // Only one is reporting "offline", we should not attempt to query it

    mockHost1.setState(HostState.SERVING);
    mockHost2.setState(HostState.OFFLINE);

    numHits = 0;

    for (int i = 0; i < 10; ++i) {
      HankResponse response = hostConnectionPool.get(mockDomain, KEY_1, 1, null);
      assertEquals(RESPONSE_1, response);
      if (response.is_set_value()) {
        ++numHits;
      }
    }
    assertEquals("Online host should receive all queries", 10, iface1.numGets);
    assertEquals("Offline host should receive no query", 0, iface2.numGets);
    assertEquals("All keys should have been found", 10, numHits);

    iface1.clearCounts();
    iface2.clearCounts();

    // Both are reporting "offline", we should attempt to query them opportunistically

    mockHost1.setState(HostState.OFFLINE);
    mockHost2.setState(HostState.OFFLINE);

    numHits = 0;

    for (int i = 0; i < 10; ++i) {
      HankResponse response = hostConnectionPool.get(mockDomain, KEY_1, 1, null);
      assertEquals(RESPONSE_1, response);
      if (response.is_set_value()) {
        ++numHits;
      }
    }
    assertEquals("Gets should be distributed accross hosts", 5, iface1.numGets);
    assertEquals("Gets should be distributed accross hosts", 5, iface2.numGets);
    assertEquals("All keys should have been found", 10, numHits);
  }

  @Test
  public void testSimplePreferredPool() throws InterruptedException, IOException {

    //  two hosts, one preferred one not.  get all requests there

    MockIface iface1 = new Response1Iface();
    MockIface iface2 = new Response1Iface();

    startMockPartitionServerThread1(iface1, 1);
    startMockPartitionServerThread2(iface2, 1);

    Map<Host, List<HostConnection>> hostToConnectionsMap = new HashMap<Host, List<HostConnection>>();

    int tryLockTimeoutMs = 0;
    int establishConnectionTimeoutMs = 0;
    int queryTimeoutMs = 0;
    int bulkQueryTimeoutMs = 0;

    mockHost1.setEnvironmentFlags(Collections.singletonMap("REGION", "REG1"));
    mockHost2.setEnvironmentFlags(Collections.singletonMap("REGION", "REG2"));

    hostToConnectionsMap.put(mockHost1, Collections.singletonList(new HostConnection(mockHost1,
        tryLockTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs)));
    hostToConnectionsMap.put(mockHost2, Collections.singletonList(new HostConnection(mockHost2,
        tryLockTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs)));

    HostConnectionPool hostConnectionPool = new HostConnectionPool(hostToConnectionsMap, null, Sets.newHashSet(mockHost1));

    mockHost1.setState(HostState.SERVING);
    mockHost2.setState(HostState.SERVING);

    int numHits = 0;

    for (int i = 0; i < 10; ++i) {
      HankResponse response = hostConnectionPool.get(mockDomain, KEY_1, 1, null);
      if (response.is_set_value()) {
        assertEquals(RESPONSE_1, response);
        ++numHits;
      }
    }

    assertEquals("Gets should all hit first host", 10, iface1.numGets);
    assertEquals("Gets should not hit second host", 0, iface2.numGets);
    assertEquals("All keys should have been found", 10, numHits);

  }

  @Test
  public void testBothPreferred() throws InterruptedException, IOException {

    MockIface iface1 = new Response1Iface();
    MockIface iface2 = new Response1Iface();

    startMockPartitionServerThread1(iface1, 1);
    startMockPartitionServerThread2(iface2, 1);

    Map<Host, List<HostConnection>> hostToConnectionsMap = new HashMap<Host, List<HostConnection>>();

    int tryLockTimeoutMs = 0;
    int establishConnectionTimeoutMs = 0;
    int queryTimeoutMs = 0;
    int bulkQueryTimeoutMs = 0;

    mockHost1.setEnvironmentFlags(Collections.singletonMap("REGION", "REG1"));
    mockHost2.setEnvironmentFlags(Collections.singletonMap("REGION", "REG1"));

    hostToConnectionsMap.put(mockHost1, Collections.singletonList(new HostConnection(mockHost1,
        tryLockTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs)));
    hostToConnectionsMap.put(mockHost2, Collections.singletonList(new HostConnection(mockHost2,
        tryLockTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs)));

    HostConnectionPool hostConnectionPool = new HostConnectionPool(hostToConnectionsMap, null, Sets.newHashSet(mockHost1, mockHost2));

    mockHost1.setState(HostState.SERVING);
    mockHost2.setState(HostState.SERVING);

    int numHits = 0;

    for (int i = 0; i < 10; ++i) {
      HankResponse response = hostConnectionPool.get(mockDomain, KEY_1, 1, null);
      if (response.is_set_value()) {
        assertEquals(RESPONSE_1, response);
        ++numHits;
      }
    }

    assertEquals("Gets should be distributed across hosts", 5, iface1.numGets);
    assertEquals("Gets should be distributed across hosts", 5, iface2.numGets);
    assertEquals("Half the keys should have been found", 10, numHits);


  }

  @Test
  public void testPreferredFailing() throws InterruptedException, IOException {

    MockIface iface1 = new HankExceptionIface();
    MockIface iface2 = new Response1Iface();

    startMockPartitionServerThread1(iface1, 1);
    startMockPartitionServerThread2(iface2, 1);

    Map<Host, List<HostConnection>> hostToConnectionsMap = new HashMap<Host, List<HostConnection>>();

    int tryLockTimeoutMs = 0;
    int establishConnectionTimeoutMs = 0;
    int queryTimeoutMs = 0;
    int bulkQueryTimeoutMs = 0;

    mockHost1.setEnvironmentFlags(Collections.singletonMap("REGION", "REG1"));
    mockHost2.setEnvironmentFlags(Collections.singletonMap("REGION", "REG1"));

    hostToConnectionsMap.put(mockHost1, Collections.singletonList(new HostConnection(mockHost1,
        tryLockTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs)));
    hostToConnectionsMap.put(mockHost2, Collections.singletonList(new HostConnection(mockHost2,
        tryLockTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs)));

    HostConnectionPool hostConnectionPool = new HostConnectionPool(hostToConnectionsMap, null, Sets.newHashSet(mockHost1));

    mockHost1.setState(HostState.SERVING);
    mockHost2.setState(HostState.SERVING);

    int numHits = 0;

    for (int i = 0; i < 10; ++i) {
      HankResponse response = hostConnectionPool.get(mockDomain, KEY_1, 2, null);
      if (response.is_set_value()) {
        assertEquals(RESPONSE_1, response);
        ++numHits;
      }
    }

    assertEquals("First should have gotten all requests once", 10, iface1.numGets);
    assertEquals("Second should have gotten all as a backup", 10, iface2.numGets);
    assertEquals("All keys found", 10, numHits);

    //  half should fail

    iface1.clearCounts();
    iface2.clearCounts();
    numHits = 0;

    for (int i = 0; i < 10; ++i) {
      HankResponse response = hostConnectionPool.get(mockDomain, KEY_1, 1, null);
      if (response.is_set_value()) {
        assertEquals(RESPONSE_1, response);
        ++numHits;
      }
    }

    assertEquals("All hosts to first", 10, iface1.numGets);
    assertEquals("None to second", 0, iface2.numGets);
    assertEquals("No keys found", 0, numHits);

  }

  @Test
  public void testPreferredFallback() throws InterruptedException, IOException {

    //  two preferred, one not.  one preferred failing.

    MockIface iface1 = new HankExceptionIface();
    MockIface iface2 = new Response1Iface();
    MockIface iface3 = new Response1Iface();

    startMockPartitionServerThread1(iface1, 1);
    startMockPartitionServerThread2(iface2, 1);
    startMockPartitionServerThread3(iface3, 1);

    Map<Host, List<HostConnection>> hostToConnectionsMap = new HashMap<Host, List<HostConnection>>();

    int tryLockTimeoutMs = 0;
    int establishConnectionTimeoutMs = 0;
    int queryTimeoutMs = 0;
    int bulkQueryTimeoutMs = 0;

    mockHost1.setEnvironmentFlags(Collections.singletonMap("REGION", "REG1"));
    mockHost2.setEnvironmentFlags(Collections.singletonMap("REGION", "REG1"));
    mockHost3.setEnvironmentFlags(Collections.singletonMap("REGION", "REG2"));


    hostToConnectionsMap.put(mockHost1, Collections.singletonList(new HostConnection(mockHost1,
        tryLockTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs)));
    hostToConnectionsMap.put(mockHost2, Collections.singletonList(new HostConnection(mockHost2,
        tryLockTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs)));
    hostToConnectionsMap.put(mockHost3, Collections.singletonList(new HostConnection(mockHost3,
        tryLockTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs)));


    HostConnectionPool hostConnectionPool = new HostConnectionPool(hostToConnectionsMap, null, Sets.newHashSet(mockHost1, mockHost2));

    mockHost1.setState(HostState.SERVING);
    mockHost2.setState(HostState.SERVING);
    mockHost3.setState(HostState.SERVING);

    iface1.clearCounts();
    iface2.clearCounts();
    iface3.clearCounts();

    int numHits = 0;

    for (int i = 0; i < 10; ++i) {
      HankResponse response = hostConnectionPool.get(mockDomain, KEY_1, 2, null);
      if (response.is_set_value()) {
        assertEquals(RESPONSE_1, response);
        ++numHits;
      }
    }

    assertEquals("Gets all fail on preferred host 1", 5, iface1.numGets);
    assertEquals("Gets should all forward to second preferred host", 10, iface2.numGets);
    assertEquals("No gets on non preferred host", 0, iface3.numGets);
    assertEquals("All keys found", 10, numHits);

  }


  @Test
  public void testOneHankExceptions() throws IOException, InterruptedException {

    MockIface iface1 = new Response1Iface();
    MockIface iface2 = new HankExceptionIface();

    startMockPartitionServerThread1(iface1, 1);
    startMockPartitionServerThread2(iface2, 1);

    Map<Host, List<HostConnection>> hostToConnectionsMap = new HashMap<Host, List<HostConnection>>();

    int tryLockTimeoutMs = 0;
    int establishConnectionTimeoutMs = 0;
    int queryTimeoutMs = 0;
    int bulkQueryTimeoutMs = 0;

    hostToConnectionsMap.put(mockHost1, Collections.singletonList(new HostConnection(mockHost1,
        tryLockTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs)));
    hostToConnectionsMap.put(mockHost2, Collections.singletonList(new HostConnection(mockHost2,
        tryLockTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs)));

    HostConnectionPool hostConnectionPool = new HostConnectionPool(hostToConnectionsMap, null, Sets.newHashSet());

    mockHost1.setState(HostState.SERVING);
    mockHost2.setState(HostState.SERVING);

    // With num retries = 1

    int numHits = 0;

    for (int i = 0; i < 10; ++i) {
      HankResponse response = hostConnectionPool.get(mockDomain, KEY_1, 1, null);
      if (response.is_set_value()) {
        assertEquals(RESPONSE_1, response);
        ++numHits;
      }
    }

    assertEquals("Gets should be distributed accross hosts", 5, iface1.numGets);
    assertEquals("Gets should be distributed accross hosts", 5, iface2.numGets);
    assertEquals("Half the keys should have been found", 5, numHits);

    iface1.clearCounts();
    iface2.clearCounts();

    // With num retries = 2

    numHits = 0;

    for (int i = 0; i < 10; ++i) {
      HankResponse response = hostConnectionPool.get(mockDomain, KEY_1, 2, null);
      assertEquals(RESPONSE_1, response);
      if (response.is_set_value()) {
        ++numHits;
      }
    }
    assertEquals("Non failing host should get all requests", 10, iface1.numGets);
    assertEquals("Gets should be distributed accross hosts", 5, iface2.numGets);
    assertEquals("All keys should have been found", 10, numHits);
  }

  @Test
  public void testOneHanging() throws IOException, InterruptedException {
    Semaphore semaphore = new Semaphore(0);
    final MockIface iface1 = new HangingIface(semaphore);
    final MockIface iface2 = new Response1Iface();

    startMockPartitionServerThread1(iface1, 1);
    startMockPartitionServerThread2(iface2, 1);

    Map<Host, List<HostConnection>> hostToConnectionsMap = new HashMap<Host, List<HostConnection>>();

    int tryLockTimeoutMs = 0;
    int establishConnectionTimeoutMs = 0;
    int queryTimeoutMs = 100;
    int bulkQueryTimeoutMs = 0;

    hostToConnectionsMap.put(mockHost1, Collections.singletonList(new HostConnection(mockHost1,
        tryLockTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs)));
    hostToConnectionsMap.put(mockHost2, Collections.singletonList(new HostConnection(mockHost2,
        tryLockTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs)));

    HostConnectionPool hostConnectionPool = new HostConnectionPool(hostToConnectionsMap, null, Sets.newHashSet());

    mockHost1.setState(HostState.SERVING);
    mockHost2.setState(HostState.SERVING);

    int numHits;
    int previousIface1NumGets;

    // With max num retries = 1
    numHits = 0;
    iface1.clearCounts();
    iface2.clearCounts();
    previousIface1NumGets = 0;
    for (int i = 0; i < 10; ++i) {
      HankResponse response = hostConnectionPool.get(mockDomain, KEY_1, 1, null);
      LOG.trace("Num retries = 1, sequence index = " + i +
          " host 1 gets = " + iface1.numGets + ", host 2 gets = " + iface2.numGets);
      if (response.is_set_value()) {
        ++numHits;
      }
      if (iface1.numGets != previousIface1NumGets) {
        semaphore.release();
        previousIface1NumGets = iface1.numGets;
      }
    }

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return iface1.numGets == 5
            && iface1.numCompletedGets == 5
            && iface2.numGets == 5
            && iface2.numCompletedGets == 5;
      }
    });

    assertEquals("Half the requests should have failed with Host 1", 5, iface1.numGets);
    assertEquals("Half the requests should have failed with Host 1", 5, iface1.numCompletedGets);
    assertEquals("Half the requests should have succeeded with Host 2", 5, iface2.numGets);
    assertEquals("Half the requests should have succeeded with Host 2", 5, iface2.numCompletedGets);
    assertEquals("Half the keys should have been found", 5, numHits);

    // With max num retries = 2
    numHits = 0;
    iface1.clearCounts();
    iface2.clearCounts();
    previousIface1NumGets = 0;
    for (int i = 0; i < 10; ++i) {
      HankResponse response = hostConnectionPool.get(mockDomain, KEY_1, 2, null);
      LOG.trace("Num retries = 2, sequence index = " + i +
          " host 1 gets = " + iface1.numGets + ", host 2 gets = " + iface2.numGets);
      assertEquals(RESPONSE_1, response);
      if (response.is_set_value()) {
        ++numHits;
      }
      if (iface1.numGets != previousIface1NumGets) {
        semaphore.release();
        previousIface1NumGets = iface1.numGets;
      }
    }

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return iface1.numGets == 5
            && iface1.numCompletedGets == 5
            && iface2.numGets == 10
            && iface2.numCompletedGets == 10;
      }
    });

    assertEquals("Half the requests should have failed with Host 1", 5, iface1.numGets);
    assertEquals("Half the requests should have failed with Host 1", 5, iface1.numCompletedGets);
    assertEquals("Host 2 should have served all requests", 10, iface2.numGets);
    assertEquals("Host 2 should have served all requests", 10, iface2.numCompletedGets);
    assertEquals("All keys should have been found", 10, numHits);
  }

  @Test
  public void testDeterministicHostListShuffling() throws IOException, InterruptedException {

    MockIface iface1 = new Response1Iface();
    MockIface iface2 = new Response1Iface();

    startMockPartitionServerThread1(iface1, 1);
    startMockPartitionServerThread2(iface2, 1);

    Map<Host, List<HostConnection>> hostToConnectionsMap = new HashMap<Host, List<HostConnection>>();

    int tryLockTimeoutMs = 0;
    int establishConnectionTimeoutMs = 0;
    int queryTimeoutMs = 0;
    int bulkQueryTimeoutMs = 0;

    hostToConnectionsMap.put(mockHost1, Collections.singletonList(new HostConnection(mockHost1,
        tryLockTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs)));
    hostToConnectionsMap.put(mockHost2, Collections.singletonList(new HostConnection(mockHost2,
        tryLockTimeoutMs, establishConnectionTimeoutMs, queryTimeoutMs, bulkQueryTimeoutMs)));

    mockHost1.setState(HostState.SERVING);
    mockHost2.setState(HostState.SERVING);

    for (int n = 0; n < 1024; ++n) {
      int numHits = 0;
      // Note: creating connection pools with a host shuffling seed
      HostConnectionPool hostConnectionPoolA = new HostConnectionPool(hostToConnectionsMap, n, Sets.newHashSet());
      HostConnectionPool hostConnectionPoolB = new HostConnectionPool(hostToConnectionsMap, n, Sets.newHashSet());

      // Connection pools should try the same host first for a given key hash
      final int keyHash = 42;
      for (int i = 0; i < 10; ++i) {
        HankResponse responseA = hostConnectionPoolA.get(mockDomain, KEY_1, 1, keyHash);
        HankResponse responseB = hostConnectionPoolB.get(mockDomain, KEY_1, 1, keyHash);
        assertEquals(RESPONSE_1, responseA);
        assertEquals(RESPONSE_1, responseB);
        if (responseA.is_set_value() && responseB.is_set_value()) {
          numHits += 2;
        }
      }
      assertNotSame("Gets should not be distributed accross hosts", iface1.numGets, iface2.numGets);
      assertTrue("All gets should have been served by one host", (iface1.numGets == 20 && iface2.numGets == 0)
          || (iface1.numGets == 0 && iface2.numGets == 20));
      assertEquals("All keys should have been found", 20, numHits);
      iface1.clearCounts();
      iface2.clearCounts();
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

  private void startMockPartitionServerThread3(IfaceWithShutdown handler, int numWorkerThreads)
      throws InterruptedException {
    mockPartitionServer3 = new TestHostConnection.MockPartitionServer(handler, numWorkerThreads,
        partitionServerAddress3);
    mockPartitionServerThread3 = new Thread(mockPartitionServer3);
    mockPartitionServerThread3.start();
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        return mockPartitionServer3.dataServer != null
            && mockPartitionServer3.dataServer.isServing();
      }
    });
  }

}
