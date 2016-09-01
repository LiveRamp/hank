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


import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.mock.MockCoordinator;
import com.liveramp.hank.partitioner.Murmur64Partitioner;
import com.liveramp.hank.storage.echo.Echo;
import com.liveramp.hank.test.ZkTestCase;
import com.liveramp.hank.util.Condition;
import com.liveramp.hank.util.WaitUntil;
import com.liveramp.hank.zookeeper.ZkPath;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestZkDomainGroup extends ZkTestCase {

  private final String domainGroupsRoot = ZkPath.append(getRoot(), "domain_groups");
  private final String domainsRoot = ZkPath.append(getRoot(), "domains");

  @Before
  public void setUp() throws Exception {
    create(domainsRoot);
    create(domainGroupsRoot);
  }

  @Test
  public void testLoad() throws Exception {

    final Domain d0 = ZkDomain.create(getZk(), domainsRoot, "domain0", 1024, Echo.Factory.class.getName(), "---",
        Murmur64Partitioner.class.getName(), 0, Collections.<String>emptyList());
    final Domain d1 = ZkDomain.create(getZk(), domainsRoot, "domain1", 1024, Echo.Factory.class.getName(), "---",
        Murmur64Partitioner.class.getName(), 1, Collections.<String>emptyList());

    Coordinator coord = new MockCoordinator() {
      @Override
      public Domain getDomainById(int domainId) {
        if (domainId == 0) {
          return d0;
        } else if (domainId == 1) {
          return d1;
        } else {
          throw new IllegalStateException();
        }
      }
    };

    final ZkDomainGroup dg = ZkDomainGroup.create(getZk(), coord, domainGroupsRoot, "dg");

    assertEquals(0, dg.getDomainVersions().size());

    Map<Domain, Integer> map1 = new HashMap<Domain, Integer>();
    map1.put(d0, 0);
    map1.put(d1, 0);
    dg.setDomainVersions(map1);

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return dg.getDomainVersions().size() != 0;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    assertEquals(2, dg.getDomainVersions().size());

    assertEquals(0, dg.getDomainVersion(d0).getVersionNumber());
    assertEquals(0, dg.getDomainVersion(d1).getVersionNumber());

    Map<Domain, Integer> map2 = new HashMap<Domain, Integer>();
    map2.put(d0, 1);
    map2.put(d1, 1);
    dg.setDomainVersions(map2);

    WaitUntil.orDie(new Condition() {
      @Override
      public boolean test() {
        try {
          return dg.getDomainVersion(d0).getVersionNumber() != 0;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    assertEquals(2, dg.getDomainVersions().size());

    assertEquals(1, dg.getDomainVersion(d0).getVersionNumber());
    assertEquals(1, dg.getDomainVersion(d1).getVersionNumber());
  }

  @Test
  public void testDelete() throws Exception {
    ZkDomainGroup dg = ZkDomainGroup.create(getZk(), null, domainGroupsRoot, "dg");
    assertNotNull(getZk().exists(dg.getPath(), false));
    assertTrue(dg.delete());
    assertNull(getZk().exists(dg.getPath(), false));
  }
}
