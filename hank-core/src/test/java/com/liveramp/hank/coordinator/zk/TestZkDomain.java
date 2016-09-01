/**
 *  Copyright 2011 LiveRamp
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
package com.liveramp.hank.coordinator.zk;

import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.partitioner.ConstantPartitioner;
import com.liveramp.hank.partitioner.Murmur64Partitioner;
import com.liveramp.hank.storage.constant.ConstantStorageEngine;
import com.liveramp.hank.test.ZkTestCase;
import com.liveramp.hank.util.Condition;
import com.liveramp.hank.util.WaitUntil;
import com.liveramp.hank.zookeeper.ZkPath;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestZkDomain extends ZkTestCase {
  private static final String CONST_PARTITIONER = ConstantPartitioner.class.getName();
  private static final String STORAGE_ENGINE_FACTORY = ConstantStorageEngine.Factory.class.getName();
  private static final String STORAGE_ENGINE_OPTS = "---\n";

  @Test
  public void testCreate() throws Exception {
    ZkDomain dc = ZkDomain.create(getZk(), getRoot(), "domain0", 1024, ConstantStorageEngine.Factory.class.getName(), "---", Murmur64Partitioner.class.getName(), 0, Collections.<String>emptyList());
    assertEquals(0, dc.getId());
    assertEquals("domain0", dc.getName());
    assertEquals(1024, dc.getNumParts());
    assertEquals(ConstantStorageEngine.Factory.class.getName(), dc.getStorageEngineFactoryClassName());
    assertEquals(ConstantStorageEngine.Factory.class, dc.getStorageEngineFactoryClass());
    assertTrue(dc.getStorageEngine() instanceof ConstantStorageEngine);
    assertTrue(dc.getVersions().isEmpty());
    assertTrue(dc.getPartitioner() instanceof Murmur64Partitioner);
  }

  @Test
  public void testLoad() throws Exception {
    ZkDomain.create(getZk(), getRoot(), "domain0", 1024, ConstantStorageEngine.Factory.class.getName(), "---", Murmur64Partitioner.class.getName(), 0, Collections.<String>emptyList());
    ZkDomain dc = new ZkDomain(getZk(), ZkPath.append(getRoot(), "domain0"));

    assertEquals(0, dc.getId());
    assertEquals("domain0", dc.getName());
    assertEquals(1024, dc.getNumParts());
    assertEquals(ConstantStorageEngine.Factory.class.getName(), dc.getStorageEngineFactoryClassName());
    assertEquals(ConstantStorageEngine.Factory.class, dc.getStorageEngineFactoryClass());
    assertTrue(dc.getStorageEngine() instanceof ConstantStorageEngine);
    assertTrue(dc.getVersions().isEmpty());
    assertTrue(dc.getPartitioner() instanceof Murmur64Partitioner);
  }

  @Test
  public void testVersioning() throws Exception {
    final ZkDomain dc = ZkDomain.create(getZk(), getRoot(), "domain0", 1, STORAGE_ENGINE_FACTORY, STORAGE_ENGINE_OPTS, CONST_PARTITIONER, 0, Collections.<String>emptyList());

    assertTrue(dc.getVersions().isEmpty());

    DomainVersion version = dc.openNewVersion(null);
    assertEquals(0, version.getVersionNumber());
    assertEquals(1, dc.getVersions().size());
    version.close();

    Thread.sleep(1000);

    version = dc.openNewVersion(null);
    assertNotNull(version);
    assertEquals(1, version.getVersionNumber());
    assertEquals(2, dc.getVersions().size());

    // Test getVersionShallow
    assertTrue(dc.getVersionShallow(0) != null);
    assertEquals(dc.getVersion(0), dc.getVersionShallow(0));
  }

  @Test
  public void testDelete() throws Exception {
    ZkDomain dc = ZkDomain.create(getZk(), getRoot(), "domain0", 1, ConstantStorageEngine.Factory.class.getName(), "---", Murmur64Partitioner.class.getName(), 0, Collections.<String>emptyList());
    assertNotNull(getZk().exists(ZkPath.append(getRoot(), "domain0"), false));
    assertTrue(dc.delete());
    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return getZk().exists(ZkPath.append(getRoot(), "domain0"), false) == null;
        } catch (KeeperException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });
    assertNull(getZk().exists(ZkPath.append(getRoot(), "domain0"), false));
  }
}
