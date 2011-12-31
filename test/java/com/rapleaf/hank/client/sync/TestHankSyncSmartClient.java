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
package com.rapleaf.hank.client.sync;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.coordinator.mock.MockCoordinator;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainGroup;
import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.PartitionServer;
import com.rapleaf.hank.partitioner.MapPartitioner;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.THsHaServer.Args;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class TestHankSyncSmartClient extends BaseTestCase {
  private static final Logger LOG = Logger.getLogger(TestHankSyncSmartClient.class);
  private Host host1;
  private Host host2;
  private MockCoordinator mockCoord;
  private PartitionServer.Iface iface1;
  private PartitionServer.Iface iface2;
  private TServer server1;
  private TNonblockingServerTransport transport1;
  private Thread thread1;
  private TNonblockingServerTransport transport2;
  private TServer server2;
  private Thread thread2;

  public void setUp() throws Exception{
    int server1Port = 12345;
    int server2Port = 12346;

    // launch server 1
    iface1 = new MockPartitionServerHandler(0, VALUE_1);
    transport1 = createPartitionServerTransport(server1Port);
    server1 = createPartitionServer(transport1, iface1);
    thread1 = new Thread(new ServerRunnable(server1), "mock partition server thread 1");
    thread1.start();

    // launch server 2;
    iface2 = new MockPartitionServerHandler(0, VALUE_2);
    transport2 = createPartitionServerTransport(server2Port);
    server2 = createPartitionServer(transport2, iface2);
    thread2 = new Thread(new ServerRunnable(server2), "mock partition server thread 2");
    thread2.start();

    final MockDomain existentDomain = new MockDomain("existent_domain", 0, 2,
            new MapPartitioner(KEY_1, 0, KEY_2, 1), null, null, null);

    host1 = getHost(existentDomain, new PartitionServerAddress("localhost",
            server1Port), 0);
    host2 = getHost(existentDomain, new PartitionServerAddress("localhost",
            server2Port), 1);

    final MockRing mockRing = new MockRing(null, null, 1, RingState.OPEN) {

      @Override
      public Set<Host> getHosts() {
        return new HashSet<Host>(Arrays.asList(host1, host2));
      }
    };

    MockDomainGroup mockDomainGroup = new MockDomainGroup("myDomainGroup") {
      private final Map<Integer, Domain> domains = new HashMap<Integer, Domain>() {
        {
          put(1, existentDomain);
        }
      };

      @Override
      public Domain getDomain(int domainId) {
        return domains.get(domainId);
      }

      @Override
      public Integer getDomainId(String domainName) {
        if (domainName.equals("existent_domain")) {
          return 1;
        } else {
          return null;
        }
      }

      @Override
      public DomainGroupVersion getVersionByNumber(int version) {
        return new MockDomainGroupVersion(
                new HashSet<DomainGroupVersionDomainVersion>(
                        Arrays.asList(new MockDomainGroupVersionDomainVersion(
                                existentDomain, 1))), this, 1);
      }
    };
    final MockRingGroup mockRingGroup = new MockRingGroup(
            mockDomainGroup, "myRingGroup", null, 0, null) {
      @Override
      public Set<Ring> getRings() {
        return Collections.singleton((Ring) mockRing);
      }
    };
    mockCoord = new MockCoordinator() {
      @Override
      public RingGroup getRingGroup(String ringGroupName) {
        return mockRingGroup;
      }

      @Override
      public Domain getDomain(String domainName) {
        if (domainName.equals("existent_domain")) {
          return existentDomain;
        } else {
          return null;
        }
      }
    };

    Thread.sleep(1000);
  }

  private static class ServerRunnable implements Runnable {
    private final TServer server;

    public ServerRunnable(TServer server) {
      this.server = server;
    }

    @Override
    public void run() {
      server.serve();
    }
  }

  private static class MockPartitionServerHandler implements PartitionServer.Iface {
    @SuppressWarnings("unused")
    private final int domainId;
    private final HankResponse response;
    private final HankBulkResponse bulkResponse;
    private Mode mode = Mode.NORMAL;

    private static enum Mode {
      NORMAL,
      HANGING,
      HARD_HANGING,
      FAILING,
      THROWING_ERROR
    }

    public MockPartitionServerHandler(int domainId, ByteBuffer result) {
      this.domainId = domainId;
      this.response = HankResponse.value(result);
      List<HankResponse> responses = new ArrayList<HankResponse>();
      responses.add(HankResponse.value(result));
      this.bulkResponse = HankBulkResponse.responses(responses);
    }

    @Override
    public HankResponse get(int domainId, ByteBuffer key) throws TException {
      applyMode();
      return response;
    }

    @Override
    public HankBulkResponse getBulk(int domainId, List<ByteBuffer> keys) throws TException {
      applyMode();
      return bulkResponse;
    }

    public void setMode(Mode mode) {
      this.mode = mode;
    }

    private void applyMode() {
      switch (mode) {
        case HANGING:
          // Simulating hanging
          try {
            Thread.sleep(5000);
            break;
          } catch (InterruptedException e) {

          }
        case HARD_HANGING:
          // Simulating hanging
          while (true) {}
        case FAILING:
          throw new RuntimeException("In failing mode.");
        case THROWING_ERROR:
          throw new Error("Throwing error mode");
      }
    }
  }

  private TNonblockingServerTransport createPartitionServerTransport(int port) {
    TNonblockingServerSocket transport = null;
    int tries = 0;
    int maxTries = 1000;
    while (tries < maxTries) {
      try {
        transport = new TNonblockingServerSocket(port);
        LOG.debug("succeeded in binding server to port " + port);
        break;
      } catch (TTransportException e) {
        LOG.debug("failed to bind to port " + port);
        port++;
      }
      ++tries;
    }
    if (tries == maxTries) {
      throw new RuntimeException("Could not not create partition server transport");
    }
    return transport;
  }

  private TServer createPartitionServer(TNonblockingServerTransport transport,
                                        PartitionServer.Iface iface) {
    Args args = new Args(transport);
    args.processor(new PartitionServer.Processor(iface));
    args.protocolFactory(new TCompactProtocol.Factory());
    return new THsHaServer(args);
  }

  private Host getHost(final Domain domain, PartitionServerAddress address, final int partNum)
      throws IOException {
    MockHost hc = new MockHost(address) {
      @Override
      public Set<HostDomain> getAssignedDomains() throws IOException {
        return Collections.singleton((HostDomain) new MockHostDomain(domain) {
          @Override
          public HostDomainPartition addPartition(int partNum, int initialVersion) {
            return null;
          }

          @Override
          public Set<HostDomainPartition> getPartitions() {
            return Collections
                .singleton((HostDomainPartition) new MockHostDomainPartition(
                    partNum, 1, -1));
          }
        });
      }
    };
    hc.setState(HostState.SERVING);
    return hc;
  }

  private static final ByteBuffer KEY_1 = ByteBuffer.wrap(new byte[]{1});
  private static final ByteBuffer VALUE_1 = ByteBuffer.wrap(new byte[]{1});
  private static final ByteBuffer KEY_2 = ByteBuffer.wrap(new byte[]{2});
  private static final ByteBuffer VALUE_2 = ByteBuffer.wrap(new byte[]{2});

  public void testGet() throws Exception {
    HankSyncSmartClient c = null;
    try {
      c = new HankSyncSmartClient(mockCoord, "myRingGroup", 1, 2, 0, 1000, 0);

      // Test invalid get
      assertEquals(HankResponse.xception(HankException.no_such_domain(true)), c.get("nonexistent_domain", null));

      // Test get
      assertEquals(HankResponse.value(VALUE_1), c.get("existent_domain", KEY_1));
      assertEquals(HankResponse.value(VALUE_2), c.get("existent_domain", KEY_2));

      // Host state change
      host1.setState(HostState.OFFLINE);
      assertEquals(HankResponse.xception(HankException.no_connection_available(true)),
              c.get("existent_domain", KEY_1));

      host2.setState(HostState.UPDATING);
      assertEquals(HankResponse.xception(HankException.no_connection_available(true)),
              c.get("existent_domain", KEY_1));
      assertEquals(HankResponse.xception(HankException.no_connection_available(true)),
              c.get("existent_domain", KEY_2));

      host1.setState(HostState.SERVING);
      host2.setState(HostState.SERVING);

      // TODO: Test not querying deletable partitions

      // Simulate servers that fail to perform gets
      ((MockPartitionServerHandler) iface1).setMode(MockPartitionServerHandler.Mode.FAILING);
      ((MockPartitionServerHandler) iface2).setMode(MockPartitionServerHandler.Mode.FAILING);
      assertTrue(c.get("existent_domain", KEY_1).get_xception().is_set_internal_error());
      assertTrue(c.get("existent_domain", KEY_2).get_xception().is_set_internal_error());

      // Simulate servers that throws an error
      ((MockPartitionServerHandler) iface1).setMode(MockPartitionServerHandler.Mode.THROWING_ERROR);
      ((MockPartitionServerHandler) iface2).setMode(MockPartitionServerHandler.Mode.THROWING_ERROR);
      assertTrue(c.get("existent_domain", KEY_1).get_xception().is_set_internal_error());
      assertTrue(c.get("existent_domain", KEY_2).get_xception().is_set_internal_error());

      // Simulate servers that hangs
      ((MockPartitionServerHandler) iface1).setMode(MockPartitionServerHandler.Mode.HANGING);
      ((MockPartitionServerHandler) iface2).setMode(MockPartitionServerHandler.Mode.HANGING);
      assertTrue(c.get("existent_domain", KEY_1).get_xception().is_set_internal_error());
      assertTrue(c.get("existent_domain", KEY_2).get_xception().is_set_internal_error());

      // Simulate servers that hangs
      //((MockPartitionServerHandler) iface1).setMode(MockPartitionServerHandler.Mode.HARD_HANGING);
      //((MockPartitionServerHandler) iface2).setMode(MockPartitionServerHandler.Mode.HARD_HANGING);
      //assertTrue(c.get("existent_domain", KEY_1).get_xception().is_set_internal_error());
      //assertTrue(c.get("existent_domain", KEY_2).get_xception().is_set_internal_error());
    } finally {
      if (c != null) {
        c.stop();
      }
      server1.stop();
      server2.stop();
      thread1.join();
      thread2.join();
      transport1.close();
      transport2.close();
    }
  }
//
//  public void testConnectionTimeout() throws Exception {
//    HankSyncSmartClient c = null;
//    try {
//      c = new HankSyncSmartClient(mockCoord, "myRingGroup", 1, 1, 0, 1000, 0);
//      // Simulate servers that hangs
//      ((MockPartitionServerHandler) iface1).setMode(MockPartitionServerHandler.Mode.HANGING);
//      ((MockPartitionServerHandler) iface2).setMode(MockPartitionServerHandler.Mode.HANGING);
//      assertTrue(c.get("existent_domain", KEY_1).get_xception().is_set_internal_error());
//      assertTrue(c.get("existent_domain", KEY_1).get_xception().is_set_internal_error());
//
//    } finally {
//      if (c != null) {
//        c.stop();
//      }
//      server1.stop();
//      server2.stop();
//      thread1.join();
//      thread2.join();
//      transport1.close();
//      transport2.close();
//    }
//  }

  /*
  public void testGetBulk() throws Exception {
    try {
      HankSyncSmartClient c = new HankSyncSmartClient(mockCoord, "myRingGroup", 1, 1, 0, 0, 0);

      // Test invalid getBulk
      assertEquals(HankBulkResponse.xception(HankException.no_such_domain(true)), c.getBulk("nonexistent_domain", null));

      // Test getBulk
      HankBulkResponse bulkResponse1 = HankBulkResponse.responses(new ArrayList<HankResponse>());
      bulkResponse1.get_responses().add(HankResponse.value(VALUE_1));
      bulkResponse1.get_responses().add(HankResponse.value(VALUE_2));
      List<ByteBuffer> bulkResquest1 = new ArrayList<ByteBuffer>();
      bulkResquest1.add(KEY_1);
      bulkResquest1.add(KEY_2);
      assertEquals(bulkResponse1, c.getBulk("existent_domain", bulkResquest1));

      // Host state change
      host1.setState(HostState.OFFLINE);
      bulkResponse1.get_responses().add(HankResponse.value(VALUE_2));

      host1.setState(HostState.SERVING);
      host2.setState(HostState.SERVING);
      bulkResponse1.get_responses().add(HankResponse.value(VALUE_1));
      bulkResponse1.get_responses().add(HankResponse.value(VALUE_2));

      // Simulate servers that throws an error
      ((MockPartitionServerHandler) iface1).setMode(MockPartitionServerHandler.Mode.THROWING_ERROR);
      ((MockPartitionServerHandler) iface2).setMode(MockPartitionServerHandler.Mode.THROWING_ERROR);

      assertEquals(HankResponse.xception(HankException.no_connection_available(true)),
          c.get("existent_domain", KEY_1));
      assertEquals(HankResponse.xception(HankException.no_connection_available(true)),
          c.get("existent_domain", KEY_2));

      // Simulate servers that hangs

      ((MockPartitionServerHandler) iface1).setMode(MockPartitionServerHandler.Mode.HANGING);
      ((MockPartitionServerHandler) iface2).setMode(MockPartitionServerHandler.Mode.HANGING);

      assertEquals(HankResponse.xception(HankException.no_connection_available(true)),
          c.get("existent_domain", KEY_1));
      assertEquals(HankResponse.xception(HankException.no_connection_available(true)),
          c.get("existent_domain", KEY_2));
    } finally {
      server1.stop();
      server2.stop();
      thread1.join();
      thread2.join();
      transport1.close();
      transport2.close();
    }
  }
  */
}
