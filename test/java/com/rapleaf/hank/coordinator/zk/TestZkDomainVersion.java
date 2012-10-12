package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.DomainVersions;
import com.rapleaf.hank.generated.PartitionMetadata;
import com.rapleaf.hank.zookeeper.ZkPath;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

public class TestZkDomainVersion extends ZkTestCase {
  public void testCreate() throws Exception {
    DomainVersion dv = ZkDomainVersion.create(getZk(), getRoot(), 1, null, null);
    assertEquals(1, dv.getVersionNumber());
    assertNull(dv.getClosedAt());
    assertFalse(DomainVersions.isClosed(dv));
    assertFalse(dv.isDefunct());
  }

  public void testLoad() throws Exception {
    ZkDomainVersion.create(getZk(), getRoot(), 1, null, null);
    DomainVersion dv = new ZkDomainVersion(getZk(), ZkPath.append(getRoot(), "v/" + 1), null);
    assertEquals(1, dv.getVersionNumber());
    assertNull(dv.getClosedAt());
    assertFalse(DomainVersions.isClosed(dv));
    assertFalse(dv.isDefunct());
  }

  public void testCancelVersion() throws Exception {
    DomainVersion dv = ZkDomainVersion.create(getZk(), getRoot(), 1, null, null);
    assertEquals(1, dv.getVersionNumber());
    assertNull(dv.getClosedAt());
    assertFalse(DomainVersions.isClosed(dv));

    dv.cancel();
    assertNull(getZk().exists(ZkPath.append(getRoot(), "v/1"), false));
  }

  public void testCloseVersion() throws Exception {
    DomainVersion dv = ZkDomainVersion.create(getZk(), getRoot(), 1, null, null);
    assertEquals(1, dv.getVersionNumber());
    assertNull(dv.getClosedAt());
    assertFalse(DomainVersions.isClosed(dv));

    dv.close();
    Thread.sleep(100);

    assertNotNull(dv.getClosedAt());
    assertTrue(DomainVersions.isClosed(dv));
  }

  public void testPartitionMetadata() throws Exception {
    DomainVersion dv = ZkDomainVersion.create(getZk(), getRoot(), 1, null, null);
    DomainVersion dv2 = new ZkDomainVersion(getZk(), ZkPath.append(getRoot(), "v/1"), null);

    assertTrue(dv.getPartitionsMetadata().isEmpty());
    assertTrue(dv2.getPartitionsMetadata().isEmpty());

    dv.addPartitionProperties(1, 2, 3);

    Thread.sleep(100);

    assertEquals(1, dv.getPartitionsMetadata().size());
    assertEquals(1, dv2.getPartitionsMetadata().size());
    PartitionMetadata metadata = dv.getPartitionsMetadata().iterator().next();
    assertEquals(2, metadata.get_num_bytes());
    assertEquals(3, metadata.get_num_records());
  }

  public void testDefunct() throws Exception {
    DomainVersion dv = ZkDomainVersion.create(getZk(), getRoot(), 1, null, null);
    DomainVersion otherDv = new ZkDomainVersion(getZk(), ZkPath.append(getRoot(), "v/1"), null);

    assertFalse(dv.isDefunct());
    assertFalse(otherDv.isDefunct());

    dv.setDefunct(true);
    Thread.sleep(1000);
    assertTrue(dv.isDefunct());
    assertTrue(otherDv.isDefunct());

    dv.setDefunct(false);
    Thread.sleep(1000);
    assertFalse(dv.isDefunct());
    assertFalse(otherDv.isDefunct());
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    getZk().create(ZkPath.append(getRoot(), NewZkDomain.VERSIONS_PATH), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }
}
