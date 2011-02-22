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
import com.rapleaf.hank.coordinator.DaemonState;
import com.rapleaf.hank.coordinator.DaemonStateChangeListener;
import com.rapleaf.hank.coordinator.DaemonType;
import com.rapleaf.hank.coordinator.DomainChangeListener;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainGroupChangeListener;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;
import com.rapleaf.hank.coordinator.RingGroupConfig;


public class TestZooKeeperCoordinator extends ZkTestCase {

  public class OmniMockListener implements RingGroupChangeListener, DomainChangeListener, DomainGroupChangeListener, DaemonStateChangeListener {
    public RingGroupConfig ringGroup;
    public boolean notified = false;

    @Override
    public void onRingGroupChange(RingGroupConfig newRingGroup) {
      this.ringGroup = newRingGroup;
      notified = true;
      synchronized (this) {
        notifyAll();
      }
    }

    public DomainGroupConfig domainGroup;

    @Override
    public void onDomainGroupChange(DomainGroupConfig newDomainGroup) {
      this.domainGroup = newDomainGroup;
      notified = true;
      synchronized (this) {
        notifyAll();
      }
    }

    public DomainConfig newDomain;

    @Override
    public void onDomainChange(DomainConfig newDomain) {
      this.newDomain = newDomain;
      notified = true;
      synchronized (this) {
        notifyAll();
      }
    }

    public String ringGroupName;
    public int ringNumber;
    public PartDaemonAddress hostName;
    public DaemonType type;
    public DaemonState newState;

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

  private static final PartDaemonAddress LOCALHOST = new PartDaemonAddress("localhost", 1);

  private final String domains_root = getRoot() + "/domains";
  private final String domain_groups_root = getRoot() + "/domain_groups";
  private final String ring_groups_root = getRoot() + "/ring_groups";
  private ZooKeeperCoordinator coord;

  public TestZooKeeperCoordinator() throws Exception {
    super();
  }

  public void testLoad() throws Exception {
    // check standard loading stuff
    assertEquals("number of loaded domain configs", 1, coord.getDomainConfigs().size());
    assertEquals("get domain by name", "domain0", coord.getDomainConfig("domain0").getName());

    assertEquals("number of loaded domain group configs", 1, coord.getDomainGroupConfigs().size());
    assertEquals("get domain group by name", "myDomainGroup", coord.getDomainGroupConfig("myDomainGroup").getName());

    assertEquals("number of loaded ring groups", 1, coord.getRingGroups().size());
    assertEquals("get ring group by name", "myRingGroup", coord.getRingGroupConfig("myRingGroup").getName());

    assertEquals("ring number", 1, coord.getRingConfig("myRingGroup", 1).getRingNumber());
  }

  public void testDaemonState() throws Exception {
    // test set/get daemon state
    assertEquals(DaemonState.STARTABLE, coord.getDaemonState("myRingGroup", 1, LOCALHOST, DaemonType.PART_DAEMON));
    coord.setDaemonState("myRingGroup", 1, LOCALHOST, DaemonType.PART_DAEMON, DaemonState.IDLE);
    assertEquals(DaemonState.IDLE, coord.getDaemonState("myRingGroup", 1, LOCALHOST, DaemonType.PART_DAEMON));

    // test being notified of daemon state change
    OmniMockListener listener = new OmniMockListener();
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
    OmniMockListener listener = new OmniMockListener();
    coord.addDomainChangeListener("domain0", listener);
    getZk().setData(domains_root + "/domain0/version", "2".getBytes(), -1);
    synchronized (listener) {
      listener.wait(1000);
    }
    assertTrue("listener wasn't notified", listener.notified);
    assertEquals("domain name", "domain0", listener.newDomain.getName());
    assertEquals("domain version", 2, listener.newDomain.getVersion());
  }

  public void testDomainGroupChangeListener() throws Exception {
    OmniMockListener listener = new OmniMockListener();
    coord.addDomainGroupChangeListener("myDomainGroup", listener);
    create(domain_groups_root + "/myDomainGroup/versions/1");
    create(domain_groups_root + "/myDomainGroup/versions/1/.complete");
    synchronized (listener) {
      listener.wait(1000);
    }
    assertTrue("listener wasn't notified", listener.notified);
    assertEquals("domain group name", "myDomainGroup", listener.domainGroup.getName());
    assertEquals("domain group versions count", 1, listener.domainGroup.getVersions().size());
  }

  public void testRingGroupChangeListener() throws Exception {
    OmniMockListener listener = new OmniMockListener();
    coord.addRingGroupChangeListener("myRingGroup", listener);
    create(ring_groups_root + "/myRingGroup/ring-002");
    create(ring_groups_root + "/myRingGroup/ring-002/version", "1");
    create(ring_groups_root + "/myRingGroup/ring-002/hosts");
    synchronized (listener) {
      listener.wait(1000);
    }
    assertTrue("listener wasn't notified", listener.notified);
    assertEquals("ring group name", "myRingGroup", listener.ringGroup.getName());
    assertEquals("number of rings", 2, listener.ringGroup.getRingConfigs().size());
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

  private ZooKeeperCoordinator getCoord() throws Exception {
    return new ZooKeeperCoordinator(getZkConnectString(), 100000000, domains_root, domain_groups_root, ring_groups_root);
  }
}
