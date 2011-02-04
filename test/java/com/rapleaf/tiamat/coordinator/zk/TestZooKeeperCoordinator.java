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
package com.rapleaf.tiamat.coordinator.zk;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.rapleaf.tiamat.coordinator.DaemonState;
import com.rapleaf.tiamat.coordinator.DaemonType;
import com.rapleaf.tiamat.coordinator.Coordinator.DaemonStateChangeListener;
import com.rapleaf.tiamat.util.ZooKeeperUtils;


public class TestZooKeeperCoordinator extends TestCase {

  private class MockCoordinator extends ZooKeeperCoordinator {
    MockCoordinator(String connectString, int sessionTimeout,
        boolean loadImmediately) throws IOException, InterruptedException {
      super(connectString, sessionTimeout, loadImmediately);
    }

    public ZooKeeper zk() {return zk;}
  }

  private class MockDaemonListener implements DaemonStateChangeListener {
    public DaemonState state;

    @Override
    public void onDaemonStateChange(String ringGroupName, int ringNumber,
        String hostName, DaemonType type, DaemonState state) {
      this.state = state;
      synchronized(this) {
        this.notifyAll();
      }
    }
  }

  private MockCoordinator coord;
  private ZooKeeper zk;

  private final String ringGroupName = "rapleaf-1";
  private final int ringNumber = 15;
  private final String hostName = "localhost";

  private final String partDaemonPath = ZooKeeperUtils.getDaemonStatusPath(ringGroupName, ringNumber, hostName, DaemonType.PART_DAEMON);
  private final String updateDaemonPath = ZooKeeperUtils.getDaemonStatusPath(ringGroupName, ringNumber, hostName, DaemonType.UPDATE_DAEMON);

  public void setUp() throws Exception {
    coord = new MockCoordinator("localhost:2181", 5000, false);
    zk = coord.zk();

    ZooKeeperUtils.deleteNodeRecursively(zk, "/tiamat");

    ZooKeeperUtils.createNodeRecursively(zk, partDaemonPath);
    ZooKeeperUtils.createNodeRecursively(zk, updateDaemonPath);
  }

  public void testStateChangeWatcher() throws KeeperException, InterruptedException {
    MockDaemonListener listener = new MockDaemonListener();
    coord.addDaemonStateChangeListener(ringGroupName, ringNumber, hostName, DaemonType.PART_DAEMON, listener);
    coord.setDaemonState(ringGroupName, ringNumber, hostName, DaemonType.PART_DAEMON, DaemonState.STARTING);
    synchronized(listener) {
      listener.wait();
    }
    assertEquals(DaemonState.STARTING, listener.state);
  }
}
