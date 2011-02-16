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

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.config.PartDaemonAddress;
import com.rapleaf.hank.coordinator.DaemonState;
import com.rapleaf.hank.coordinator.DaemonType;
import com.rapleaf.hank.coordinator.Coordinator.DaemonStateChangeListener;


public class TestZooKeeperCoordinator extends ZkTestCase {

  private static final PartDaemonAddress LOCALHOST = new PartDaemonAddress("localhost", 1);

  public class MockDaemonStateChangeListener implements DaemonStateChangeListener {
    public boolean notified = false;
    private String ringGroupName;
    private int ringNumber;
    private PartDaemonAddress hostName;
    private DaemonType type;
    private DaemonState newState;

    @Override
    public void onDaemonStateChange(String ringGroupName, int ringNumber, PartDaemonAddress hostName, DaemonType type, DaemonState newState) {
      this.ringGroupName = ringGroupName;
      this.ringNumber = ringNumber;
      this.hostName = hostName;
      this.type = type;
      this.newState = newState;
      notified = true;
      synchronized (this) {
        notifyAll();
      }
    }

  }

  private final String domains_root = getRoot() + "/domains";
  private final String domain_groups_root = getRoot() + "/domain_groups";
  private final String ring_groups_root = getRoot() + "/ring_groups";

  public TestZooKeeperCoordinator() throws Exception {
    super();
  }

//  private class MockDaemonListener implements DaemonStateChangeListener {
//    public DaemonState state;
//
//    @Override
//    public void onDaemonStateChange(String ringGroupName, int ringNumber,
//        PartDaemonAddress hostAddress, DaemonType type, DaemonState state) {
//      this.state = state;
//      synchronized(this) {
//        this.notifyAll();
//      }
//    }
//  }
//public void testStateChangeWatcher() throws KeeperException, InterruptedException {
//fail("not implemented");
////MockDaemonListener listener = new MockDaemonListener();
////coord.addDaemonStateChangeListener(ringGroupName, ringNumber, hostAddress, DaemonType.PART_DAEMON, listener);
////coord.setDaemonState(ringGroupName, ringNumber, hostAddress, DaemonType.PART_DAEMON, DaemonState.STARTING);
////synchronized(listener) {
////  listener.wait();
////}
////assertEquals(DaemonState.STARTING, listener.state);
//}

  public void testEverything() throws Exception {
    create(domains_root);
    createMockDomain(domains_root + "/domain0");
    create(domain_groups_root);
    create(domain_groups_root + "/myDomainGroup");
    create(domain_groups_root + "/myDomainGroup/domains");
    create(domain_groups_root + "/myDomainGroup/versions");
    create(ring_groups_root);
    create(ring_groups_root + "/myRingGroup", domain_groups_root + "/myDomainGroup");
    create(ring_groups_root + "/myRingGroup/ring-001");
    create(ring_groups_root + "/myRingGroup/ring-001/version", "1");
    create(ring_groups_root + "/myRingGroup/ring-001/hosts");
    create(ring_groups_root + "/myRingGroup/ring-001/hosts/localhost:1");
    create(ring_groups_root + "/myRingGroup/ring-001/hosts/localhost:1/part_daemon");
    create(ring_groups_root + "/myRingGroup/ring-001/hosts/localhost:1/part_daemon/status", DaemonState.STARTABLE.name());
    create(ring_groups_root + "/myRingGroup/ring-001/hosts/localhost:1/parts");

    ZooKeeperCoordinator coord = new ZooKeeperCoordinator(getZkConnectString(), 100000, domains_root, domain_groups_root, ring_groups_root);

    // check standard loading stuff
    assertEquals("number of loaded domain configs", 1, coord.getDomainConfigs().size());
    assertEquals("get domain by name", "domain0", coord.getDomainConfig("domain0").getName());

    assertEquals("number of loaded domain group configs", 1, coord.getDomainGroupConfigs().size());
    assertEquals("get domain group by name", "myDomainGroup", coord.getDomainGroupConfig("myDomainGroup").getName());

    assertEquals("number of loaded ring groups", 1, coord.getRingGroups().size());
    assertEquals("get ring group by name", "myRingGroup", coord.getRingGroupConfig("myRingGroup").getName());

    // test set/get daemon state
    assertEquals(DaemonState.STARTABLE, coord.getDaemonState("myRingGroup", 1, LOCALHOST, DaemonType.PART_DAEMON));
    coord.setDaemonState("myRingGroup", 1, LOCALHOST, DaemonType.PART_DAEMON, DaemonState.IDLE);
    assertEquals(DaemonState.IDLE, coord.getDaemonState("myRingGroup", 1, LOCALHOST, DaemonType.PART_DAEMON));

    // test being notified of daemon state change
    MockDaemonStateChangeListener listener = new MockDaemonStateChangeListener();
    coord.addDaemonStateChangeListener("myRingGroup", 1, LOCALHOST, DaemonType.PART_DAEMON, listener);
    coord.setDaemonState("myRingGroup", 1, LOCALHOST, DaemonType.PART_DAEMON, DaemonState.STARTED);
    synchronized (listener) {
      listener.wait();
    }
    assertTrue("daemon state change listener notified", listener.notified);
    assertEquals("myRingGroup", listener.ringGroupName);
    assertEquals(1, listener.ringNumber);
    assertEquals(LOCALHOST, listener.hostName);
    assertEquals(DaemonType.PART_DAEMON, listener.type);
    assertEquals(DaemonState.STARTED, listener.newState);

    fail("assertions not fully implemented");
  }
}
