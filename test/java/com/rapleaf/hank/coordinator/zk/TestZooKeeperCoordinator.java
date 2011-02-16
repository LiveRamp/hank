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

import org.apache.zookeeper.KeeperException;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.config.PartDaemonAddress;
import com.rapleaf.hank.coordinator.DaemonState;
import com.rapleaf.hank.coordinator.DaemonType;
import com.rapleaf.hank.coordinator.Coordinator.DaemonStateChangeListener;


public class TestZooKeeperCoordinator extends ZkTestCase {

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

  public void testLoad() throws Exception {
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

    ZooKeeperCoordinator coord = new ZooKeeperCoordinator(getZkConnectString(), 100000, domains_root, domain_groups_root, ring_groups_root);

    assertEquals("number of loaded domain configs", 1, coord.getDomainConfigs().size());
    assertEquals("get domain by name", "domain0", coord.getDomainConfig("domain0").getName());

    assertEquals("number of loaded domain group configs", 1, coord.getDomainGroupConfigs().size());
    assertEquals("get domain group by name", "myDomainGroup", coord.getDomainGroupConfig("myDomainGroup").getName());

    assertEquals("number of loaded ring groups", 1, coord.getRingGroups().size());
    assertEquals("get ring group by name", "myRingGroup", coord.getRingGroupConfig("myRingGroup").getName());

    fail("assertions not fully implemented");
  }

  public void testStateChangeWatcher() throws KeeperException, InterruptedException {
    fail("not implemented");
//    MockDaemonListener listener = new MockDaemonListener();
//    coord.addDaemonStateChangeListener(ringGroupName, ringNumber, hostAddress, DaemonType.PART_DAEMON, listener);
//    coord.setDaemonState(ringGroupName, ringNumber, hostAddress, DaemonType.PART_DAEMON, DaemonState.STARTING);
//    synchronized(listener) {
//      listener.wait();
//    }
//    assertEquals(DaemonState.STARTING, listener.state);
  }
}
