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
package com.rapleaf.tiamat.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.thrift.TException;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.THsHaServer.Options;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.rapleaf.tiamat.config.DomainConfig;
import com.rapleaf.tiamat.config.DomainConfigImpl;
import com.rapleaf.tiamat.config.DomainGroupConfig;
import com.rapleaf.tiamat.config.PartDaemonConfigurator;
import com.rapleaf.tiamat.config.RingConfig;
import com.rapleaf.tiamat.config.RingGroupConfig;
import com.rapleaf.tiamat.config.RingGroupConfigImpl;
import com.rapleaf.tiamat.coordinator.Coordinator;
import com.rapleaf.tiamat.coordinator.DaemonState;
import com.rapleaf.tiamat.coordinator.DaemonType;
import com.rapleaf.tiamat.coordinator.RingState;
import com.rapleaf.tiamat.exception.DataNotFoundException;
import com.rapleaf.tiamat.generated.Tiamat;
import com.rapleaf.tiamat.generated.TiamatResponse;
import com.rapleaf.tiamat.generated.Tiamat.Iface;
import com.rapleaf.tiamat.partitioner.Partitioner;
import com.rapleaf.tiamat.util.Bytes;

public class TestHandler extends TestCase {
  
  private static final String RING_GROUP_NAME = "rapleaf";
  private static final int RING_NUMBER = 1;
  
  private class MockRingConfig implements RingConfig {

    @Override
    public Set<String> getHosts() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<String> getHostsForPartition(int domainId, int partId) {
      // Used to test out the no_such_domain response
      if (domainId == 0) {
        return new ArrayList<String>();
      }
      else {
        List<String> list = new ArrayList<String>();
        list.add("localhost");
        return list;
      }
    }

    @Override
    public int getNumber() {
      return RING_NUMBER;
    }

    @Override
    public Set<Integer> getPartitionsForHost(String hostName, int domainId)
        throws DataNotFoundException {
      return null;
    }

    @Override
    public Map<String, Map<Integer, Set<Integer>>> getPartsMap() {
      return null;
    }

    @Override
    public String getRingGroupName() {
      return RING_GROUP_NAME;
    }

    @Override
    public RingState getState() {
      return RingState.AVAILABLE;
    }
  }
  
  private class MockCoordinator implements Coordinator {
    
    private RingGroupConfig rg;
    private DomainConfig domain;

    public MockCoordinator() {
      Map<Integer, RingConfig> ringMap = new HashMap<Integer, RingConfig>();
      ringMap.put(1, new MockRingConfig());
      rg = new RingGroupConfigImpl(RING_GROUP_NAME, null, ringMap);
      
      domain = new DomainConfigImpl("rapleaf_domain", 2, new Partitioner() {
        @Override
        public int partition(ByteBuffer key) {
          return key.getInt(0);
        }
      }, null, 1);
    }
    
    @Override
    public void addDaemonStateChangeListener(String ringGroupName,
        int ringNumber, String hostName, DaemonType type,
        DaemonStateChangeListener listener) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void addDomainChangeListener(String domainName,
        DomainChangeListener listener) throws DataNotFoundException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void addDomainGroupChangeListener(String domainGroupName,
        DomainGroupChangeListener listener) throws DataNotFoundException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void addRingGroupChangeListener(String ringGroupName,
        RingGroupChangeListener listener) throws DataNotFoundException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public DaemonState getDaemonState(String ringGroupName, int ringNumber,
        String hostName, DaemonType type) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public DomainConfig getDomainConfig(String domainName)
        throws DataNotFoundException {
      return domain;
    }

    @Override
    public DomainGroupConfig getDomainGroupConfig(String domainGroupName)
        throws DataNotFoundException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public RingConfig getRingConfig(String ringGroupName, int ringNumber)
        throws DataNotFoundException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public RingGroupConfig getRingGroupConfig(String ringGroupName)
        throws DataNotFoundException {
      return rg;
    }

    @Override
    public void setDaemonState(String ringGroupName, int ringNumber,
        String hostName, DaemonType type, DaemonState state) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public int updateDomain(String domainName) throws DataNotFoundException {
      // TODO Auto-generated method stub
      return 0;
    }
    
  }
  
  private static final int SERVICE_PORT_1 = 9090;
  
  private class MockPartDaemonConfigurator1 implements PartDaemonConfigurator {
    
    private Coordinator coord = new MockCoordinator();

    @Override
    public int getNumThreads() {
      return 3;
    }

    @Override
    public int getServicePort() {
      return SERVICE_PORT_1;
    }

    @Override
    public Set<String> getLocalDataDirectories() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Coordinator getCoordinator() {
      return coord;
    }

    @Override
    public String getRingGroupName() {
      return RING_GROUP_NAME;
    }

    @Override
    public int getRingNumber() {
      // TODO Auto-generated method stub
      return RING_NUMBER;
    }
  }
 
  private class MockHandler implements Iface {
    @Override
    public TiamatResponse get(byte domainId, ByteBuffer key) throws TException {
      return TiamatResponse.value(Bytes.intToBytes(123));
    }
  }
  
  private TServer server;
  private Thread serverThread;
  
  @Override
  protected void setUp() throws Exception {
    // launch the thrift server
    final PartDaemonConfigurator configurator = new MockPartDaemonConfigurator1();
    final Iface handler = new MockHandler();
    serverThread = new Thread() {
      public void run() {
        TNonblockingServerSocket serverSocket;
        try {
          serverSocket = new TNonblockingServerSocket(configurator.getServicePort());
        } catch (TTransportException e) {
          throw new RuntimeException(e);
        }
        Options options = new Options();
        options.workerThreads = configurator.getNumThreads();
        server = new THsHaServer(new Tiamat.Processor(handler), serverSocket, options);
        server.serve();
      }
    };
    serverThread.start();
  }
  
  public void testGet() throws Exception {
    PartDaemonConfigurator configurator = new MockPartDaemonConfigurator1();
    Handler client = new Handler(configurator.getCoordinator(), configurator.getRingGroupName());
    TiamatResponse response = client.get((byte) 1, ByteBuffer.wrap(new byte[4]).putInt(1));
    
    assertEquals(Bytes.bytesToInt(response.getValue()), 123);
    assertEquals(client.get((byte)0, ByteBuffer.wrap(new byte[4]).putInt(1)).getNot_found(), true);
  }
  
  @Override
  protected void tearDown() throws Exception {
    server.stop();
    serverThread.join();
  }
}
