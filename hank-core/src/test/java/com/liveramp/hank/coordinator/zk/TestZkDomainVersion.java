package com.liveramp.hank.coordinator.zk;

import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.DomainVersions;
import com.liveramp.hank.generated.PartitionMetadata;
import com.liveramp.hank.test.ZkTestCase;
import com.liveramp.hank.util.Condition;
import com.liveramp.hank.util.WaitUntil;
import com.liveramp.hank.zookeeper.ZkPath;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestZkDomainVersion extends ZkTestCase {

  @Test
  public void testCreate() throws Exception {
    DomainVersion dv = ZkDomainVersion.create(getZk(), getRoot(), 1, null, null);
    assertEquals(1, dv.getVersionNumber());
    assertNull(dv.getClosedAt());
    assertFalse(DomainVersions.isClosed(dv));
    assertFalse(dv.isDefunct());
  }

  @Test
  public void testLoad() throws Exception {
    ZkDomainVersion.create(getZk(), getRoot(), 1, null, null);
    DomainVersion dv = new ZkDomainVersion(getZk(), ZkPath.append(getRoot(), "v/" + 1), null);
    assertEquals(1, dv.getVersionNumber());
    assertNull(dv.getClosedAt());
    assertFalse(DomainVersions.isClosed(dv));
    assertFalse(dv.isDefunct());
  }

  @Test
  public void testCancelVersion() throws Exception {
    DomainVersion dv = ZkDomainVersion.create(getZk(), getRoot(), 1, null, null);
    assertEquals(1, dv.getVersionNumber());
    assertNull(dv.getClosedAt());
    assertFalse(DomainVersions.isClosed(dv));

    dv.cancel();
    assertNull(getZk().exists(ZkPath.append(getRoot(), "v/1"), false));
  }

  @Test
  public void testCloseVersion() throws Exception {
    final DomainVersion dv = ZkDomainVersion.create(getZk(), getRoot(), 1, null, null);
    assertEquals(1, dv.getVersionNumber());
    assertNull(dv.getClosedAt());
    assertFalse(DomainVersions.isClosed(dv));

    dv.close();
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return dv.getClosedAt() != null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
    assertNotNull(dv.getClosedAt());
    assertTrue(DomainVersions.isClosed(dv));
  }

  @Test
  public void testPartitionMetadata() throws Exception {
    final DomainVersion dv = ZkDomainVersion.create(getZk(), getRoot(), 1, null, null);
    final DomainVersion dv2 = new ZkDomainVersion(getZk(), ZkPath.append(getRoot(), "v/1"), null);

    assertTrue(dv.getPartitionsMetadata().isEmpty());
    assertTrue(dv2.getPartitionsMetadata().isEmpty());

    dv.addPartitionProperties(1, 2, 3);

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return dv.getPartitionsMetadata().size() == 1 &&
              dv2.getPartitionsMetadata().size() == 1;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    assertEquals(1, dv.getPartitionsMetadata().size());
    assertEquals(1, dv2.getPartitionsMetadata().size());
    PartitionMetadata metadata = dv.getPartitionsMetadata().iterator().next();
    assertEquals(2, metadata.get_num_bytes());
    assertEquals(3, metadata.get_num_records());
  }

  @Test
  public void testDefunct() throws Exception {
    final DomainVersion dv = ZkDomainVersion.create(getZk(), getRoot(), 1, null, null);
    final DomainVersion otherDv = new ZkDomainVersion(getZk(), ZkPath.append(getRoot(), "v/1"), null);

    assertFalse(dv.isDefunct());
    assertFalse(otherDv.isDefunct());

    dv.setDefunct(true);
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return dv.isDefunct() && otherDv.isDefunct();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
    assertTrue(dv.isDefunct());
    assertTrue(otherDv.isDefunct());

    dv.setDefunct(false);
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return !dv.isDefunct() && !otherDv.isDefunct();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
    assertFalse(dv.isDefunct());
    assertFalse(otherDv.isDefunct());
  }

  @Before
  public void setUp() throws Exception {
    getZk().create(ZkPath.append(getRoot(), ZkDomain.VERSIONS_PATH), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }
}
