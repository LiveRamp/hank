package com.rapleaf.hank.coordinator.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.coordinator.DomainVersion;

public class TestZkDomainVersion extends ZkTestCase {
  public void testCreate() throws Exception {
    DomainVersion dv = ZkDomainVersion.create(getZk(), getRoot(), 1);
    assertEquals(1, dv.getVersionNumber());
    assertNull(dv.getClosedAt());
    assertFalse(dv.isClosed());
    assertFalse(dv.isDefunct());
  }

  public void testLoad() throws Exception {
    ZkDomainVersion.create(getZk(), getRoot(), 1);
    dumpZk();
    DomainVersion dv = new ZkDomainVersion(getZk(), getRoot() + "/versions/version_" + 1);
    assertEquals(1, dv.getVersionNumber());
    assertNull(dv.getClosedAt());
    assertFalse(dv.isClosed());
    assertFalse(dv.isDefunct());
  }

  public void testCancelVersion() throws Exception {
    DomainVersion dv = ZkDomainVersion.create(getZk(), getRoot(), 1);
    assertEquals(1, dv.getVersionNumber());
    assertNull(dv.getClosedAt());
    assertFalse(dv.isClosed());

    dv.cancel();
    assertNull(getZk().exists(getRoot() + "/versions/version_1", false));
  }

  public void testCloseVersion() throws Exception {
    DomainVersion dv = ZkDomainVersion.create(getZk(), getRoot(), 1);
    assertEquals(1, dv.getVersionNumber());
    assertNull(dv.getClosedAt());
    assertFalse(dv.isClosed());

    dv.close();

    assertNotNull(dv.getClosedAt());
    assertTrue(dv.isClosed());
  }

  public void testPartitionInfos() throws Exception {
    DomainVersion dv = ZkDomainVersion.create(getZk(), getRoot(), 1);
    DomainVersion dv2 = new ZkDomainVersion(getZk(), getRoot() + "/versions/version_1");

    assertTrue(dv.getPartitionInfos().isEmpty());
    assertTrue(dv2.getPartitionInfos().isEmpty());

    dv.addPartitionInfo(1, 2, 3);
    // should be available immediately
    assertEquals(1, dv.getPartitionInfos().size());
    // should be available after a short wait
    Thread.sleep(1000);
    assertEquals(1, dv2.getPartitionInfos().size());
  }

  public void testDefunct() throws Exception {
    DomainVersion dv = ZkDomainVersion.create(getZk(), getRoot(), 1);
    DomainVersion otherDv = new ZkDomainVersion(getZk(), getRoot() + "/versions/version_1");

    assertFalse(dv.isDefunct());
    assertFalse(otherDv.isDefunct());

    dv.setDefunct(true);
    assertTrue(dv.isDefunct());
    Thread.sleep(1000);

    assertTrue(otherDv.isDefunct());
    dv.setDefunct(false);

    assertFalse(dv.isDefunct());
    Thread.sleep(1000);
    assertFalse(otherDv.isDefunct());
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    getZk().create(getRoot() + "/versions", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }
}
