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
package com.rapleaf.hank.client;

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
import com.rapleaf.hank.util.Condition;
import com.rapleaf.hank.util.WaitUntil;
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

public class TestHankSmartClient extends BaseTestCase {

  private static final Logger LOG = Logger.getLogger(TestHankSmartClient.class);

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
        case FAILING:
          throw new RuntimeException("In failing mode.");
        case THROWING_ERROR:
          throw new Error("Throwing error mode");
      }
    }
  }

  private static final ByteBuffer KEY_1 = ByteBuffer.wrap(new byte[]{1});
  private static final ByteBuffer VALUE_1 = ByteBuffer.wrap(new byte[]{1});
  private static final ByteBuffer KEY_2 = ByteBuffer.wrap(new byte[]{2});
  private static final ByteBuffer VALUE_2 = ByteBuffer.wrap(new byte[]{2});
  private static final ByteBuffer KEY_3 = ByteBuffer.wrap(new byte[]{3});
  private static final ByteBuffer VALUE_3 = ByteBuffer.wrap(new byte[]{3});

  public void testIt() throws Exception {
    int server1Port = 12345;
    int server2Port = 12346;
    int server3Port = 12347;

    // launch server 1
    final PartitionServer.Iface iface1 = new MockPartitionServerHandler(0, VALUE_1);
    TNonblockingServerTransport transport1 = createPartitionServerTransport(server1Port);
    final TServer server1 = createPartitionServer(transport1, iface1);
    Thread thread1 = new Thread(new ServerRunnable(server1), "mock partition server thread 1");
    thread1.start();

    // launch server 2;
    final PartitionServer.Iface iface2 = new MockPartitionServerHandler(0, VALUE_2);
    TNonblockingServerTransport transport2 = createPartitionServerTransport(server2Port);
    final TServer server2 = createPartitionServer(transport2, iface2);
    Thread thread2 = new Thread(new ServerRunnable(server2), "mock partition server thread 2");
    thread2.start();

    // launch server 3;
    final PartitionServer.Iface iface3 = new MockPartitionServerHandler(1, VALUE_3);
    TNonblockingServerTransport transport3 = createPartitionServerTransport(server3Port);
    final TServer server3 = createPartitionServer(transport3, iface3);
    Thread thread3 = new Thread(new ServerRunnable(server3), "mock partition server thread 3");
    thread3.start();

    final MockDomain existentDomain = new MockDomain("existent_domain", 0, 2,
        new MapPartitioner(KEY_1, 0, KEY_2, 1), null, null, null);
    final MockDomain newDomain = new MockDomain("new_domain", 1, 1,
        new MapPartitioner(KEY_3, 0), null, null, null);

    final Host host1 = getHost(existentDomain, new PartitionServerAddress("localhost",
        server1Port), 0);
    final Host host2 = getHost(existentDomain, new PartitionServerAddress("localhost",
        server2Port), 1);
    final Host host3 = getHost(newDomain, new PartitionServerAddress("localhost",
        server3Port), 0);

    final Set<Host> mockRingHosts = new HashSet<Host>() {{
      add(host1);
      add(host2);
    }};

    final MockRing mockRing = new MockRing(null, null, 1) {

      @Override
      public Set<Host> getHosts() {
        return mockRingHosts;
      }
    };

    MockDomainGroup mockDomainGroup = new MockDomainGroup("myDomainGroup") {
      private final Map<Integer, Domain> domains = new HashMap<Integer, Domain>() {
        {
          put(1, existentDomain);
          put(2, newDomain);
        }
      };

      @Override
      public Set<DomainGroupDomainVersion> getDomainVersions() {
        return new HashSet<DomainGroupDomainVersion>(Arrays.asList(new DomainGroupDomainVersion(existentDomain, 1)));
      }
    };
    final MockRingGroup mockRingGroup = new MockRingGroup(
        mockDomainGroup, "myRingGroup", null) {
      @Override
      public Set<Ring> getRings() {
        return Collections.singleton((Ring) mockRing);
      }
    };
    Coordinator mockCoord = new MockCoordinator() {
      @Override
      public RingGroup getRingGroup(String ringGroupName) {
        return mockRingGroup;
      }

      @Override
      public Domain getDomain(String domainName) {
        if (domainName.equals("existent_domain")) {
          return existentDomain;
        } else if (domainName.equals("new_domain")) {
          return newDomain;
        } else {
          return null;
        }
      }
    };

    WaitUntil.condition(new Condition() {
      @Override
      public boolean test() {
        return server1.isServing() && server2.isServing() && server3.isServing();
      }
    });

    try {
      final HankSmartClient client = new HankSmartClient(mockCoord, "myRingGroup", 1, 1, 0, 0, 1000, 0);

      // Test invalid get
      assertEquals(HankResponse.xception(HankException.no_such_domain(true)), client.get("nonexistent_domain", null));

      // Test get
      assertEquals(HankResponse.value(VALUE_1), client.get("existent_domain", KEY_1));
      assertEquals(HankResponse.value(VALUE_2), client.get("existent_domain", KEY_2));

      // Test invalid getBulk
      assertEquals(HankBulkResponse.xception(HankException.no_such_domain(true)), client.getBulk("nonexistent_domain", null));

      // Test getBulk
      HankBulkResponse bulkResponse1 = HankBulkResponse.responses(new ArrayList<HankResponse>());
      bulkResponse1.get_responses().add(HankResponse.value(VALUE_1));
      bulkResponse1.get_responses().add(HankResponse.value(VALUE_2));
      List<ByteBuffer> bulkRequest1 = new ArrayList<ByteBuffer>();
      bulkRequest1.add(KEY_1);
      bulkRequest1.add(KEY_2);
      assertEquals(bulkResponse1, client.getBulk("existent_domain", bulkRequest1));

      // Test get with null key
      try {
        client.get("existent_domain", null);
        fail("Should throw an exception.");
      } catch (NullKeyException e) {
        // Good
      }

      // Test get with empty key
      try {
        client.get("existent_domain", ByteBuffer.wrap(new byte[0]));
        fail("Should throw an exception.");
      } catch (EmptyKeyException e) {
        // Good
      }

      // Test get bulk with null keys
      try {
        List<ByteBuffer> bulkRequest = new ArrayList<ByteBuffer>();
        bulkRequest.add(KEY_1);
        bulkRequest.add(null);
        client.getBulk("existent_domain", bulkRequest);
        fail("Should throw an exception.");
      } catch (NullKeyException e) {
        // Good
      }

      // Test get bulk with empty keys
      try {
        List<ByteBuffer> bulkRequest = new ArrayList<ByteBuffer>();
        bulkRequest.add(KEY_1);
        bulkRequest.add(ByteBuffer.wrap(new byte[0]));
        client.getBulk("existent_domain", bulkRequest);
        fail("Should throw an exception.");
      } catch (EmptyKeyException e) {
        // Good
      }

      // Host state change
      host1.setState(HostState.OFFLINE);
      assertEquals(HankResponse.xception(HankException.no_connection_available(true)),
          client.get("existent_domain", KEY_1));
      bulkResponse1.get_responses().add(HankResponse.value(VALUE_2));

      host2.setState(HostState.UPDATING);
      assertEquals(HankResponse.xception(HankException.no_connection_available(true)),
          client.get("existent_domain", KEY_1));
      assertEquals(HankResponse.xception(HankException.no_connection_available(true)),
          client.get("existent_domain", KEY_2));

      host1.setState(HostState.SERVING);
      host2.setState(HostState.SERVING);
      bulkResponse1.get_responses().add(HankResponse.value(VALUE_1));
      bulkResponse1.get_responses().add(HankResponse.value(VALUE_2));

      // Test location changes

      // Add new host that has new domain
      mockRingHosts.add(host3);

      // Should not be able to query new domain
      assertTrue(client.get("new_domain", KEY_3).get_xception().is_set_internal_error());

      // Notify client of data location change
      client.onDataLocationChange(mockCoord.getRingGroup("myRingGroup"));

      // Should be able to query new domain when the client has done updating its cache
      WaitUntil.condition(new Condition() {
        @Override
        public boolean test() {
          return HankResponse.value(VALUE_3).equals(client.get("new_domain", KEY_3));
        }
      });
      assertEquals(HankResponse.value(VALUE_3), client.get("new_domain", KEY_3));

      // TODO: Test not querying deletable partitions

      // Simulate servers that fail to perform gets
      ((MockPartitionServerHandler) iface1).setMode(MockPartitionServerHandler.Mode.FAILING);
      ((MockPartitionServerHandler) iface2).setMode(MockPartitionServerHandler.Mode.FAILING);

      assertTrue(client.get("existent_domain", KEY_1).get_xception().get_failed_retries() > 0);
      assertTrue(client.get("existent_domain", KEY_2).get_xception().get_failed_retries() > 0);

      // Simulate servers that throws an error
      ((MockPartitionServerHandler) iface1).setMode(MockPartitionServerHandler.Mode.THROWING_ERROR);
      ((MockPartitionServerHandler) iface2).setMode(MockPartitionServerHandler.Mode.THROWING_ERROR);

      assertTrue(client.get("existent_domain", KEY_1).get_xception().get_failed_retries() > 0);
      assertTrue(client.get("existent_domain", KEY_2).get_xception().get_failed_retries() > 0);

      // Simulate servers that hangs
      ((MockPartitionServerHandler) iface1).setMode(MockPartitionServerHandler.Mode.HANGING);
      ((MockPartitionServerHandler) iface2).setMode(MockPartitionServerHandler.Mode.HANGING);

      assertTrue(client.get("existent_domain", KEY_1).get_xception().get_failed_retries() > 0);
      assertTrue(client.get("existent_domain", KEY_2).get_xception().get_failed_retries() > 0);
    } finally {
      server1.stop();
      server2.stop();
      thread1.join();
      thread2.join();
      transport1.close();
      transport2.close();
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
          public HostDomainPartition addPartition(int partNum) {
            return null;
          }

          @Override
          public Set<HostDomainPartition> getPartitions() {
            return Collections
                .singleton((HostDomainPartition) new MockHostDomainPartition(
                    partNum, 1));
          }
        });
      }
    };
    hc.setState(HostState.SERVING);
    return hc;
  }
}
