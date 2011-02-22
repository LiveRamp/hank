package com.rapleaf.hank.coordinator.zk;

import java.util.Collections;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;

public class TestRingConfigImpl extends ZkTestCase {
  private static final PartDaemonAddress LOCALHOST = PartDaemonAddress.parse("localhost:1");

  public TestRingConfigImpl() throws Exception {
    super();
  }

  private final String ring_root = getRoot() + "/ring-group-one/ring-1";

  public void testLoadNotUpdating() throws Exception {
    create(ring_root + "/hosts/localhost:1");
    create(ring_root + "/hosts/localhost:1/parts");
    create(ring_root + "/hosts/localhost:1/parts/0", "1");
    create(ring_root + "/hosts/localhost:1/parts/0/1", "1");
    create(ring_root + "/hosts/localhost:1/part_daemon");
    create(ring_root + "/hosts/localhost:1/part_daemon/status", "STARTED");
    create(ring_root + "/hosts/localhost:1/update_daemon");
    create(ring_root + "/hosts/localhost:1/update_daemon/status", "IDLE");
    create(ring_root + "/current_version", "1");

    RingConfig ringConf = new RingConfigImpl(getZk(), ring_root, null);

    assertEquals("expected ring number", 1, ringConf.getRingNumber());
    assertEquals("version number", 1, ringConf.getVersionNumber());
    assertFalse("should not be updating", ringConf.isUpdatePending());
    assertEquals("number of hosts", 1, ringConf.getHosts().size());
    assertEquals("primary host address", LOCALHOST, ringConf.getHosts().toArray()[0]);
    assertEquals("assigned parts", Collections.singleton(1), ringConf.getDomainPartitionsForHost(LOCALHOST, 0));
    assertEquals("hosts for parts", Collections.singleton(LOCALHOST), ringConf.getHostsForDomainPartition(0, 1));
  }

  public void testLoadUpdating() throws Exception {
    create(ring_root + "/current_version", "1");
    create(ring_root + "/updating_to_version", "2");

    RingConfig ringConf = new RingConfigImpl(getZk(), ring_root, null);

    assertEquals("expected ring number", 1, ringConf.getRingNumber());
    assertEquals("version number", 1, ringConf.getVersionNumber());
    assertTrue("should be updating", ringConf.isUpdatePending());
    assertEquals("updating_to_version number", 2, ringConf.getUpdatingToVersionNumber());
  }

  public void testUpdateComplete() throws Exception {
    create(ring_root + "/current_version", "1");
    create(ring_root + "/updating_to_version", "2");

    RingConfig ringConf = new RingConfigImpl(getZk(), ring_root, null);

    assertEquals("expected ring number", 1, ringConf.getRingNumber());
    assertEquals("version number", 1, ringConf.getVersionNumber());
    assertTrue("should be updating", ringConf.isUpdatePending());
    assertEquals("updating_to_version number", 2, ringConf.getUpdatingToVersionNumber());

    ringConf.updateComplete();

    ringConf = new RingConfigImpl(getZk(), ring_root, null);

    assertEquals("expected ring number", 1, ringConf.getRingNumber());
    assertEquals("version number", 2, ringConf.getVersionNumber());
    assertFalse("should not be updating", ringConf.isUpdatePending());
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    create(getRoot() + "/ring-group-one");
    create(ring_root);
    create(ring_root + "/hosts");
  }
}
