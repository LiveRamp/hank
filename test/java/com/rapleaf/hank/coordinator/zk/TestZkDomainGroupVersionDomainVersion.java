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
import com.rapleaf.hank.coordinator.DomainGroupVersionDomainVersion;
import com.rapleaf.hank.zookeeper.ZkPath;

public class TestZkDomainGroupVersionDomainVersion extends ZkTestCase {
  private final String path = ZkPath.append(getRoot(), "myDomain");

  public TestZkDomainGroupVersionDomainVersion() throws Exception {
    super();
  }

  public void testLoad() throws Exception {
    create(path, "7");
    DomainGroupVersionDomainVersion dcv = new ZkDomainGroupVersionDomainVersion(getZk(), path, null);
    assertEquals(7, dcv.getVersionOrAction().getVersion());
  }
}
