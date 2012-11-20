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

package com.rapleaf.hank.monitor;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.MockRingGroup;
import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainGroup;
import com.rapleaf.hank.generated.DomainGroupMetadata;
import com.rapleaf.hank.monitor.notification.DomainGroupChangeNotification;
import com.rapleaf.hank.monitor.notification.RingGroupConductorModeNotification;
import com.rapleaf.hank.monitor.notifier.Notifier;
import com.rapleaf.hank.monitor.notifier.mock.MockNotifier;
import com.rapleaf.hank.ring_group_conductor.RingGroupConductorMode;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TestRingGroupMonitor extends BaseTestCase {


  private MockDomainGroup domainGroup = new MockDomainGroup("domain_group");
  private RingGroup mockRingGroup = new MockRingGroup(domainGroup, "ring_group", Collections.<Ring>emptySet());
  private MockNotifier mockNotifier;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    mockNotifier = new MockNotifier();
  }

  public void testMain() throws IOException {
    RingGroupMonitor monitor = new RingGroupMonitor(mockRingGroup, Collections.singletonList((Notifier) mockNotifier));

    mockRingGroup.claimRingGroupConductor(RingGroupConductorMode.INACTIVE);
    assertEquals(1, mockNotifier.getNotifications().size());
    assertTrue(mockNotifier.getNotifications().contains(
        new RingGroupConductorModeNotification(mockRingGroup, RingGroupConductorMode.INACTIVE)));

    mockRingGroup.setRingGroupConductorMode(RingGroupConductorMode.ACTIVE);
    assertEquals(2, mockNotifier.getNotifications().size());
    assertTrue(mockNotifier.getNotifications().contains(
        new RingGroupConductorModeNotification(mockRingGroup, RingGroupConductorMode.ACTIVE)));

    Domain domain = new MockDomain("domain");
    DomainGroupMetadata domainGroupMetadata = new DomainGroupMetadata();
    domainGroupMetadata.put_to_domain_versions_map(domain.getId(), 1);
    Map<Domain, Integer> domainVersions = new HashMap<Domain, Integer>();
    domainVersions.put(domain, 1);

    mockRingGroup.getDomainGroup().setDomainVersions(domainVersions);
    assertEquals(3, mockNotifier.getNotifications().size());
    assertTrue(mockNotifier.getNotifications().contains(
        new DomainGroupChangeNotification(mockRingGroup)));

    // Clear notifications
    mockNotifier.getNotifications().clear();
    mockRingGroup.setRingGroupConductorMode(RingGroupConductorMode.INACTIVE);

    assertEquals(1, mockNotifier.getNotifications().size());
    assertTrue(mockNotifier.getNotifications().contains(
        new RingGroupConductorModeNotification(mockRingGroup, RingGroupConductorMode.INACTIVE)));
  }
}
