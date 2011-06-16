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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.rapleaf.hank.ZkTestCase;

public class TestZkHostDomainPartition extends ZkTestCase {
  public void testIt() throws Exception {
    ZkHostDomainPartition hdpc = ZkHostDomainPartition.create(getZk(), getRoot(), 1234, 7);
    assertEquals(1234, hdpc.getPartNum());
    assertNull("current version should be unset", hdpc.getCurrentDomainGroupVersion());
    assertEquals(Integer.valueOf(7), hdpc.getUpdatingToDomainGroupVersion());

    hdpc.setCurrentDomainGroupVersion(7);
    assertEquals(Integer.valueOf(7), hdpc.getCurrentDomainGroupVersion());

    hdpc.setUpdatingToDomainGroupVersion(8);
    assertEquals(Integer.valueOf(8), hdpc.getUpdatingToDomainGroupVersion());

    hdpc.setUpdatingToDomainGroupVersion(null);
    assertNull(hdpc.getUpdatingToDomainGroupVersion());
    
    List<String> currentCountKeys = new ArrayList<String>();
    hdpc.setCount("TotalHits", 45);
    currentCountKeys.add("TotalHits");
    assertEquals(new Long(45), hdpc.getCount("TotalHits"));
    
    hdpc.setCount("RandomCount", 1);
    currentCountKeys.add("RandomCount");
    assertTrue(hdpc.getCountKeys().containsAll(currentCountKeys) && hdpc.getCountKeys().size() == currentCountKeys.size());
    
    hdpc.removeCount("RandomCount");
    currentCountKeys.remove("RandomCount");
    assertTrue(hdpc.getCountKeys().containsAll(currentCountKeys) && hdpc.getCountKeys().size() == currentCountKeys.size());
    
    assertNotNull(hdpc.getCount("TotalHits"));
    
    assertNull(hdpc.getCount("RandomCount"));
    
    assertNull(hdpc.getCount("NewCount"));
    
  }
}
