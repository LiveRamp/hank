package com.rapleaf.hank.coordinator.zk;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostDomainConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.PartDaemonState;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.UpdateDaemonState;

public class TestZkRingConfig extends ZkTestCase {
  private static final PartDaemonAddress LOCALHOST = PartDaemonAddress.parse("localhost:1");

  private final String ring_root = getRoot() + "/ring-group-one/ring-1";

  public void testLoadNotUpdating() throws Exception {
    ZkHostConfig hostConfig = ZkHostConfig.create(getZk(), ring_root + "/hosts", LOCALHOST);
    create(ring_root + "/current_version", "1");

    RingConfig ringConf = new ZkRingConfig(getZk(), ring_root, null);

    assertEquals("expected ring number", 1, ringConf.getRingNumber());
    assertEquals("version number", Integer.valueOf(1), ringConf.getVersionNumber());
    assertFalse("should not be updating", ringConf.isUpdatePending());
    assertEquals("number of hosts", 1, ringConf.getHosts().size());
    assertEquals("expected hosts", Collections.singleton(hostConfig), ringConf.getHosts());
  }

  public void testLoadUpdating() throws Exception {
    create(ring_root + "/current_version", "1");
    create(ring_root + "/updating_to_version", "2");

    RingConfig ringConf = new ZkRingConfig(getZk(), ring_root, null);

    assertEquals("expected ring number", 1, ringConf.getRingNumber());
    assertEquals("version number", Integer.valueOf(1), ringConf.getVersionNumber());
    assertTrue("should be updating", ringConf.isUpdatePending());
    assertEquals("updating_to_version number", Integer.valueOf(2), ringConf.getUpdatingToVersionNumber());
  }

  public void testUpdateComplete() throws Exception {
    create(ring_root + "/current_version", "1");
    create(ring_root + "/updating_to_version", "2");

    RingConfig ringConf = new ZkRingConfig(getZk(), ring_root, null);

    assertEquals("expected ring number", 1, ringConf.getRingNumber());
    assertEquals("version number", Integer.valueOf(1), ringConf.getVersionNumber());
    assertTrue("should be updating", ringConf.isUpdatePending());
    assertEquals("updating_to_version number", Integer.valueOf(2), ringConf.getUpdatingToVersionNumber());

    ringConf.updateComplete();

    ringConf = new ZkRingConfig(getZk(), ring_root, null);

    assertEquals("expected ring number", 1, ringConf.getRingNumber());
    assertEquals("version number", Integer.valueOf(2), ringConf.getVersionNumber());
    assertFalse("should not be updating", ringConf.isUpdatePending());
  }

  public void testHosts() throws Exception {
    create(ring_root + "/current_version", "1");

    ZkRingConfig ringConf = new ZkRingConfig(getZk(), ring_root, null);
    assertEquals(0, ringConf.getHosts().size());

    HostConfig hc = ringConf.addHost(LOCALHOST);
    assertEquals(LOCALHOST, hc.getAddress());
    assertEquals(Collections.singleton(hc), ringConf.getHosts());

    assertEquals(LOCALHOST, ringConf.getHostConfigByAddress(LOCALHOST).getAddress());
  }

  public void testStartAllPartDaemons() throws Exception {
    create(ring_root + "/current_version", "1");
    ZkHostConfig hc = ZkHostConfig.create(getZk(), ring_root + "/hosts", LOCALHOST);
    assertEquals(PartDaemonState.IDLE, hc.getPartDaemonState());
    ZkRingConfig rc = new ZkRingConfig(getZk(), ring_root, null);
    rc.startAllPartDaemons();
    assertEquals(PartDaemonState.STARTABLE, hc.getPartDaemonState());
  }

  public void testTakeDownAllPartDaemons() throws Exception {
    create(ring_root + "/current_version", "1");
    ZkHostConfig hc = ZkHostConfig.create(getZk(), ring_root + "/hosts", LOCALHOST);
    hc.setPartDaemonState(PartDaemonState.STARTED);
    assertEquals(PartDaemonState.STARTED, hc.getPartDaemonState());
    ZkRingConfig rc = new ZkRingConfig(getZk(), ring_root, null);
    rc.takeDownPartDaemons();
    assertEquals(PartDaemonState.STOPPABLE, hc.getPartDaemonState());
  }

  public void testStartAllUpdaters() throws Exception {
    create(ring_root + "/current_version", "1");
    ZkHostConfig hc = ZkHostConfig.create(getZk(), ring_root + "/hosts", LOCALHOST);
    assertEquals(UpdateDaemonState.IDLE, hc.getUpdateDaemonState());
    ZkRingConfig rc = new ZkRingConfig(getZk(), ring_root, null);
    rc.startAllUpdaters();
    assertEquals(UpdateDaemonState.UPDATABLE, hc.getUpdateDaemonState());
  }

  public void testGetOldestVersionOnHosts() throws Exception {
    create(ring_root + "/current_version", "1");
    ZkHostConfig hc = ZkHostConfig.create(getZk(), ring_root + "/hosts", LOCALHOST);
    HostDomainConfig d = hc.addDomain(0);
    d.addPartition(1, 1).setCurrentDomainGroupVersion(1);
    d = hc.addDomain(1);
    d.addPartition(1, 2).setCurrentDomainGroupVersion(2);
    ZkRingConfig rc = new ZkRingConfig(getZk(), ring_root, null);
    assertEquals(Integer.valueOf(1), rc.getOldestVersionOnHosts());
  }

  public void testGetHostsForDomainPartition() throws Exception {
    RingConfig rc = ZkRingConfig.create(getZk(), getRoot(), 1, null, 1);
    PartDaemonAddress h1 = new PartDaemonAddress("localhost", 1);
    PartDaemonAddress h2 = new PartDaemonAddress("localhost", 2);
    PartDaemonAddress h3 = new PartDaemonAddress("localhost", 3);

    HostConfig hc1 = rc.addHost(h1);
    HostDomainConfig d = hc1.addDomain(1);
    d.addPartition(0, 1);
    HostConfig hc2 = rc.addHost(h2);
    d = hc2.addDomain(1);
    d.addPartition(1, 1);
    HostConfig hc3 = rc.addHost(h3);
    d = hc3.addDomain(1);
    d.addPartition(2, 1);
    d.addPartition(0, 1);

    assertEquals(new HashSet<HostConfig>(Arrays.asList(hc1, hc3)), rc.getHostsForDomainPartition(1, 0));
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    create(getRoot() + "/ring-group-one");
    create(ring_root);
    create(ring_root + "/hosts");
  }
}
