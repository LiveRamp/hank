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
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.partitioner.ConstantPartitioner;
import com.rapleaf.hank.partitioner.Murmur64Partitioner;
import com.rapleaf.hank.storage.constant.ConstantStorageEngine;

public class TestZkDomain extends ZkTestCase {
  private static final String CONST_PARTITIONER = ConstantPartitioner.class.getName();
  private static final String STORAGE_ENGINE_FACTORY = ConstantStorageEngine.Factory.class.getName();
  private static final String STORAGE_ENGINE_OPTS = "---\n";

  public void testCreate() throws Exception {
    ZkDomain dc = ZkDomain.create(getZk(), getRoot(), "domain0", 1024, ConstantStorageEngine.Factory.class.getName(), "---", Murmur64Partitioner.class.getName());
    assertEquals("domain0", dc.getName());
    assertEquals(1024, dc.getNumParts());
    assertEquals(ConstantStorageEngine.Factory.class.getName(), dc.getStorageEngineFactoryName());
    assertEquals(ConstantStorageEngine.Factory.class, dc.getStorageEngineFactoryClass());
    assertTrue(dc.getStorageEngine() instanceof ConstantStorageEngine);
    assertTrue(dc.getVersions().isEmpty());
    assertTrue(dc.getPartitioner() instanceof Murmur64Partitioner);
  }

  public void testLoad() throws Exception {
    ZkDomain.create(getZk(), getRoot(), "domain0", 1024, ConstantStorageEngine.Factory.class.getName(), "---", Murmur64Partitioner.class.getName());
    ZkDomain dc = new ZkDomain(getZk(), getRoot() + "/domain0");

    assertEquals("domain0", dc.getName());
    assertEquals(1024, dc.getNumParts());
    assertEquals(ConstantStorageEngine.Factory.class.getName(), dc.getStorageEngineFactoryName());
    assertEquals(ConstantStorageEngine.Factory.class, dc.getStorageEngineFactoryClass());
    assertTrue(dc.getStorageEngine() instanceof ConstantStorageEngine);
    assertTrue(dc.getVersions().isEmpty());
    assertTrue(dc.getPartitioner() instanceof Murmur64Partitioner);
  }

  public void testVersioning() throws Exception {
    ZkDomain dc = ZkDomain.create(getZk(), getRoot(), "domain0", 1, STORAGE_ENGINE_FACTORY, STORAGE_ENGINE_OPTS, CONST_PARTITIONER);

    assertTrue(dc.getVersions().isEmpty());

    DomainVersion version = dc.openNewVersion();
    assertEquals(0, version.getVersionNumber());
    assertEquals(1, dc.getVersions().size());

    assertNull(dc.openNewVersion());
    assertEquals(1, dc.getVersions().size());

    version.close();

    version = dc.openNewVersion();
    assertNotNull(version);
    assertEquals(1, version.getVersionNumber());
    assertEquals(2, dc.getVersions().size());
  }

  public void testDelete() throws Exception {
    ZkDomain dc = ZkDomain.create(getZk(), getRoot(), "domain0", 1, ConstantStorageEngine.Factory.class.getName(), "---", Murmur64Partitioner.class.getName());
    assertNotNull(getZk().exists(getRoot() + "/domain0", false));
    assertTrue(dc.delete());
    assertNull(getZk().exists(getRoot() + "/domain0", false));
  }
}
