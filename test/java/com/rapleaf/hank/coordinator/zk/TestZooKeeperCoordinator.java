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
import com.rapleaf.hank.config.DomainConfig;
import com.rapleaf.hank.config.PartDaemonAddress;
import com.rapleaf.hank.coordinator.DaemonState;
import com.rapleaf.hank.coordinator.DaemonStateChangeListener;
import com.rapleaf.hank.coordinator.DaemonType;
import com.rapleaf.hank.coordinator.DomainChangeListener;


public class TestZooKeeperCoordinator extends ZkTestCase {

  public class MockDomainChangeListener implements DomainChangeListener {
    public boolean notified = false;
    public DomainConfig newDomain;

    @Override
    public void onDomainChange(DomainConfig newDomain) {
      this.newDomain = newDomain;
      notified = true;
      synchronized (this) {
        notifyAll();
      }
    }
  }

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
  private ZooKeeperCoordinator coord;

  public TestZooKeeperCoordinator() throws Exception {
    super();
  }

  public void testLoad() throws Exception {
    ZooKeeperCoordinator coord = getCoord();

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
  }

  public void testDaemonState() throws Exception {
    ZooKeeperCoordinator coord = getCoord();

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
  }

  public void testDomainChangeListener() throws Exception {
    MockDomainChangeListener listener = new MockDomainChangeListener();
    coord.addDomainChangeListener("domain0", listener);
    getZk().setData(domains_root + "/domain0/version", "2".getBytes(), -1);
    synchronized (listener) {
      listener.wait(10000);
    }
    assertTrue("listener wasn't notified", listener.notified);
    assertEquals("domain name", "domain0", listener.newDomain.getName());
    assertEquals("domain version", 2, listener.newDomain.getVersion());
  }

  public void testDomainGroupChangeListener() throws Exception {
    fail();
  }

  public void testRingGroupChangeListener() throws Exception {
    fail();
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();

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

    coord = getCoord();
  }

  private ZooKeeperCoordinator getCoord() throws InterruptedException {
    return new ZooKeeperCoordinator(getZkConnectString(), 100000000, domains_root, domain_groups_root, ring_groups_root);
  }
}
