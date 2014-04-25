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

package com.liveramp.hank.monitor;

import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.monitor.notification.HostStateNotification;
import com.liveramp.hank.monitor.notifier.Notifier;
import com.liveramp.hank.monitor.notifier.mock.MockNotifier;
import com.liveramp.hank.test.BaseTestCase;
import com.liveramp.hank.test.coordinator.MockHost;
import com.liveramp.hank.test.coordinator.MockRing;
import com.liveramp.hank.test.coordinator.MockRingGroup;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHostMonitor extends BaseTestCase {

  private RingGroup mockRingGroup = new MockRingGroup(null, "rg", null);
  private Ring mockRing = new MockRing(null, mockRingGroup, 0);
  private Host mockHost;
  private MockNotifier mockNotifier;

  @Before
  public void setUp() throws Exception {
    mockHost = new MockHost(new PartitionServerAddress("", 0));
    mockNotifier = new MockNotifier();
  }

  @Test
  public void testMain() throws IOException {
    HostMonitor monitor = new HostMonitor(mockRingGroup, mockRing, mockHost, Collections.singletonList((Notifier) mockNotifier));

    mockHost.setState(HostState.IDLE);
    assertEquals(0, mockNotifier.getNotifications().size());

    mockHost.setState(HostState.OFFLINE);
    assertEquals(1, mockNotifier.getNotifications().size());
    assertTrue(mockNotifier.getNotifications().contains(
        new HostStateNotification(mockRingGroup, mockRing, mockHost, HostState.OFFLINE)));
  }
}
