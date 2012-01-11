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
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.monitor.notification.HostStateNotification;
import com.rapleaf.hank.monitor.notifier.MockNotifier;

import java.io.IOException;

public class TestHostMonitor extends BaseTestCase {

  private RingGroup mockRingGroup = new MockRingGroup(null, "rg", null, null);
  private Ring mockRing = new MockRing(null, mockRingGroup, 0);
  private Host mockHost;
  private MockNotifier mockNotifier;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    mockHost = new MockHost(new PartitionServerAddress("", 0));
    mockNotifier = new MockNotifier();
  }

  public void testMain() throws IOException {
    HostMonitor monitor = new HostMonitor(mockRingGroup, mockRing, mockHost, mockNotifier);

    mockHost.setState(HostState.IDLE);
    assertEquals(1, mockNotifier.getNotifications().size());
    assertTrue(mockNotifier.getNotifications().contains(
        new HostStateNotification(mockRingGroup, mockRing, mockHost, HostState.IDLE)));

    mockHost.setState(HostState.OFFLINE);
    assertEquals(2, mockNotifier.getNotifications().size());
    assertTrue(mockNotifier.getNotifications().contains(
        new HostStateNotification(mockRingGroup, mockRing, mockHost, HostState.OFFLINE)));
  }
}
