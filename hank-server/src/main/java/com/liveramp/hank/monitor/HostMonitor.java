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
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.monitor.notification.HostStateNotification;
import com.liveramp.hank.monitor.notifier.Notification;
import com.liveramp.hank.monitor.notifier.Notifier;
import com.liveramp.hank.zookeeper.WatchedNodeListener;

import java.io.IOException;
import java.util.List;

public class HostMonitor {

  private final RingGroup ringGroup;
  private final Ring ring;
  private final Host host;
  private final List<Notifier> notifiers;
  private final WatchedNodeListener<HostState> hostStateListener;

  public HostMonitor(RingGroup ringGroup,
                     Ring ring,
                     Host host,
                     List<Notifier> notifiers) throws IOException {
    this.ringGroup = ringGroup;
    this.ring = ring;
    this.host = host;
    this.notifiers = notifiers;

    hostStateListener = new HostStateMonitor();
    host.setStateChangeListener(hostStateListener);
  }

  private class HostStateMonitor implements WatchedNodeListener<HostState> {
    @Override
    public void onWatchedNodeChange(HostState state) {
      // No state means OFFLINE
      if (state == null) {
        state = HostState.OFFLINE;
      }
      if (state == HostState.OFFLINE) {
        doNotify(new HostStateNotification(ringGroup, ring, host, state));
      }
    }
  }

  private void doNotify(Notification notification) {
    for (Notifier notifier : notifiers) {
      notifier.doNotify(notification);
    }
  }

  public void stop() {
    host.cancelStateChangeListener(hostStateListener);
  }
}
