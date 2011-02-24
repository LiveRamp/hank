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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.thrift.TException;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.THsHaServer.Options;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.rapleaf.hank.config.PartDaemonConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainChangeListener;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainGroupChangeListener;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.coordinator.RingState;
import com.rapleaf.hank.coordinator.zk.DomainConfigImpl;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.SmartClient;
import com.rapleaf.hank.generated.SmartClient.Iface;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.util.Bytes;

public class TestHankSmartClient extends TestCase {
  private static final String RING_GROUP_NAME = "rapleaf";
  private static final int RING_NUMBER = 1;

  private class MockRingConfig implements RingConfig {

    @Override
    public Set<HostConfig> getHosts() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public int getRingNumber() {
      return RING_NUMBER;
    }

    @Override
    public RingState getState() {
      return RingState.AVAILABLE;
    }

    @Override
    public RingGroupConfig getRingGroupConfig() {
      return null;
    }

    @Override
    public Integer getVersionNumber() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public Integer getUpdatingToVersionNumber() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public boolean isUpdatePending() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void startAllPartDaemons() {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void startAllUpdaters() {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void takeDownPartDaemons() {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void updateComplete() {
      // TODO Auto-generated method stub
      
    }

    @Override
    public int getOldestVersionOnHosts() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public HostConfig addHost(PartDaemonAddress address) throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public HostConfig getHostConfigByAddress(PartDaemonAddress address) {
      // TODO Auto-generated method stub
      return null;
    }
  }

  private class MockCoordinator implements Coordinator {
    private RingGroupConfig rg;
    private DomainConfig domain;

    public MockCoordinator() {
      Map<Integer, RingConfig> ringMap = new HashMap<Integer, RingConfig>();
      final MockRingConfig mockRingConfig = new MockRingConfig();
      ringMap.put(1, mockRingConfig);
      rg = new RingGroupConfig() {
        @Override
        public DomainGroupConfig getDomainGroupConfig() {
          return null;
        }

        @Override
        public String getName() {
          return RING_GROUP_NAME;
        }

        @Override
        public RingConfig getRingConfig(int ringNumber)
            throws DataNotFoundException {
          return mockRingConfig;
        }

        @Override
        public RingConfig getRingConfigForHost(PartDaemonAddress hostAddress)
            throws DataNotFoundException {
          return mockRingConfig;
        }

        @Override
        public Set<RingConfig> getRingConfigs() {
          return Collections.singleton((RingConfig)mockRingConfig);
        }

        @Override
        public boolean claimDataDeployer() {
          // TODO Auto-generated method stub
          return false;
        }

        @Override
        public void releaseDataDeployer() {
          // TODO Auto-generated method stub
          
        }

        @Override
        public int getCurrentVersion() {
          // TODO Auto-generated method stub
          return 0;
        }

        @Override
        public boolean isUpdating() {
          // TODO Auto-generated method stub
          return false;
        }

        @Override
        public void setUpdatingToVersion(int versionNumber) {
          // TODO Auto-generated method stub
          
        }

        @Override
        public void updateComplete() {
          // TODO Auto-generated method stub
          
        }
      };

      domain = new DomainConfigImpl("rapleaf_domain", 2, new Partitioner() {
        @Override
        public int partition(ByteBuffer key) {
          return key.getInt(0);
        }
      }, null, 1);
    }

    @Override
    public void addDomainGroupChangeListener(String domainGroupName,
        DomainGroupChangeListener listener)
    throws DataNotFoundException {
    }

    @Override
    public void addRingGroupChangeListener(String ringGroupName,
        RingGroupChangeListener listener) throws DataNotFoundException {
    }

    @Override
    public DomainConfig getDomainConfig(String domainName)
        throws DataNotFoundException {
      return domain;
    }

    @Override
    public DomainGroupConfig getDomainGroupConfig(String domainGroupName)
        throws DataNotFoundException {
      return null;
    }

    @Override
    public RingGroupConfig getRingGroupConfig(String ringGroupName)
        throws DataNotFoundException {
      return rg;
    }

    @Override
    public int updateDomain(String domainName) throws DataNotFoundException {
      return 0;
    }

    @Override
    public Set<DomainConfig> getDomainConfigs() {
      // TODO Auto-generated method stub
      return null;
    }

    public Set<DomainGroupConfig> getDomainGroupConfigs() {
      return null;
    }

    public Set<RingGroupConfig> getRingGroups() {
      return null;
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
      return RING_NUMBER;
    }
  }

  private class MockHandler implements Iface {
    @Override
    public HankResponse get(String domain_name, ByteBuffer key) throws TException {
      return HankResponse.value(Bytes.intToBytes(123));
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
        server = new THsHaServer(new SmartClient.Processor(handler), serverSocket, options);
        server.serve();
      }
    };
    serverThread.start();
  }

  public void testGet() throws Exception {
    PartDaemonConfigurator configurator = new MockPartDaemonConfigurator1();
    HankSmartClient client = new HankSmartClient(configurator.getCoordinator(), configurator.getRingGroupName());
    HankResponse response = client.get("", ByteBuffer.wrap(new byte[4]).putInt(1));

    assertEquals(Bytes.bytesToInt(response.get_value()), 123);
    assertEquals(client.get("", ByteBuffer.wrap(new byte[4]).putInt(1)).get_not_found(), true);
  }

  @Override
  protected void tearDown() throws Exception {
    server.stop();
    serverThread.join();
  }
}
