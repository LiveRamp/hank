package com.rapleaf.hank.coordinator.zk;

import java.util.Collections;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.config.PartDaemonAddress;
import com.rapleaf.hank.config.RingConfig;

public class TestRingConfigImpl extends ZkTestCase {
  private static final PartDaemonAddress LOCALHOST = PartDaemonAddress.parse("localhost:1");

  public TestRingConfigImpl() throws Exception {
    super();
  }

  private final String ring_root = getRoot() + "/ring-group-one/ring-1";

  public void testLoad() throws Exception {
    create(getRoot() + "/ring-group-one");
    create(ring_root);
    create(ring_root + "/hosts");
    create(ring_root + "/hosts/localhost:1");
    create(ring_root + "/hosts/localhost:1/parts");
    create(ring_root + "/hosts/localhost:1/parts/0", "1");
    create(ring_root + "/hosts/localhost:1/parts/0/1", "1");
    create(ring_root + "/hosts/localhost:1/part_daemon");
    create(ring_root + "/hosts/localhost:1/part_daemon/status", "STARTED");
    create(ring_root + "/hosts/localhost:1/update_daemon");
    create(ring_root + "/hosts/localhost:1/update_daemon/status", "IDLE");
//    create(ring_root + "/hosts/localhost:2");
    create(ring_root + "/version", "1");

    RingConfig ringConf = new RingConfigImpl(getZk(), ring_root, null);

    assertEquals("expected ring number", 1, ringConf.getRingNumber());
    assertEquals("version number", 1, ringConf.getVersionNumber());
    assertEquals("number of hosts", 1, ringConf.getHosts().size());
    assertEquals("primary host address", LOCALHOST, ringConf.getHosts().toArray()[0]);
    assertEquals("assigned parts", Collections.singleton(1), ringConf.getDomainPartitionsForHost(LOCALHOST, 0));
    assertEquals("hosts for parts", Collections.singleton(LOCALHOST), ringConf.getHostsForDomainPartition(0, 1));

    // TODO: test code for determining state of a ring
  }
}
