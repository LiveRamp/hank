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
import com.rapleaf.hank.zookeeper.ZkPath;

public class TestZkHostDomainPartition extends ZkTestCase {
  public void testIt() throws Exception {
    ZkHostDomainPartition hdpc = ZkHostDomainPartition.create(getZk(), getRoot(), 1234, null);
    Thread.sleep(10);
    assertEquals(1234, hdpc.getPartitionNumber());
    assertNull("current version should be unset", hdpc.getCurrentDomainGroupVersion());

    hdpc.setCurrentDomainGroupVersion(7);
    Thread.sleep(10);
    assertEquals(Integer.valueOf(7), hdpc.getCurrentDomainGroupVersion());

    assertEquals(false, hdpc.isDeletable());
    hdpc.setDeletable(true);
    ZkHostDomainPartition hdpc2 = new ZkHostDomainPartition(getZk(), ZkPath.append(getRoot(), Integer.toString(1234)), null);
    Thread.sleep(10);
    assertEquals(true, hdpc2.isDeletable());

    hdpc.delete();
    Thread.sleep(10);
    assertNull(getZk().exists(hdpc.getPath(), false));
  }
}
