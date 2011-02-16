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
package com.rapleaf.hank.coordinator.zk;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.config.PartDaemonAddress;
import com.rapleaf.hank.coordinator.DaemonState;
import com.rapleaf.hank.coordinator.DaemonType;
import com.rapleaf.hank.coordinator.Coordinator.DaemonStateChangeListener;
import com.rapleaf.hank.util.ZooKeeperUtils;


public class TestZooKeeperCoordinator extends ZkTestCase {

  public TestZooKeeperCoordinator() throws Exception {
    super();
  }

  private class MockCoordinator extends ZooKeeperCoordinator {
    MockCoordinator(String connectString, int sessionTimeout,
        boolean loadImmediately) throws IOException, InterruptedException {
      super(connectString, sessionTimeout, loadImmediately);
    }
  }

  private class MockDaemonListener implements DaemonStateChangeListener {
    public DaemonState state;

    @Override
    public void onDaemonStateChange(String ringGroupName, int ringNumber,
        PartDaemonAddress hostAddress, DaemonType type, DaemonState state) {
      this.state = state;
      synchronized(this) {
        this.notifyAll();
      }
    }
  }

  private MockCoordinator coord;

  private final String ringGroupName = "rapleaf-1";
  private final int ringNumber = 15;
  private final String hostName = "localhost";
  private final PartDaemonAddress hostAddress = new PartDaemonAddress(hostName, 12345);

  private final String partDaemonPath = ZooKeeperUtils.getDaemonStatusPath(ringGroupName, ringNumber, hostAddress, DaemonType.PART_DAEMON);
  private final String updateDaemonPath = ZooKeeperUtils.getDaemonStatusPath(ringGroupName, ringNumber, hostAddress, DaemonType.UPDATE_DAEMON);

  public void setUp() throws Exception {
    super.setUp();
    coord = new MockCoordinator("localhost:" + getZkClientPort(), 5000, false);

    ZooKeeperUtils.deleteNodeRecursively(getZk(), "/tiamat");

    ZooKeeperUtils.createNodeRecursively(getZk(), partDaemonPath);
    ZooKeeperUtils.createNodeRecursively(getZk(), updateDaemonPath);
  }

  public void testStateChangeWatcher() throws KeeperException, InterruptedException {
    MockDaemonListener listener = new MockDaemonListener();
    coord.addDaemonStateChangeListener(ringGroupName, ringNumber, hostAddress, DaemonType.PART_DAEMON, listener);
    coord.setDaemonState(ringGroupName, ringNumber, hostAddress, DaemonType.PART_DAEMON, DaemonState.STARTING);
    synchronized(listener) {
      listener.wait();
    }
    assertEquals(DaemonState.STARTING, listener.state);
  }
}
