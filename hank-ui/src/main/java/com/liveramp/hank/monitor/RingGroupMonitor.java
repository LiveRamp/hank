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

import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.DomainGroupListener;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.monitor.notification.DomainGroupChangeNotification;
import com.liveramp.hank.monitor.notification.RingGroupConductorModeNotification;
import com.liveramp.hank.monitor.notifier.Notification;
import com.liveramp.hank.monitor.notifier.Notifier;
import com.liveramp.hank.ring_group_conductor.RingGroupConductorMode;
import com.liveramp.hank.zookeeper.WatchedNodeListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RingGroupMonitor {

  private final RingGroup ringGroup;
  private final List<Notifier> notifiers;
  private Collection<RingMonitor> ringMonitors = new ArrayList<RingMonitor>();
  private final RingGroupConductorModeMonitor ringGroupConductorStatusMonitor;
  private final DomainGroupMetadataMonitor domainGroupMetadataMonitor;

  private class RingGroupConductorModeMonitor implements WatchedNodeListener<RingGroupConductorMode> {

    @Override
    public void onWatchedNodeChange(RingGroupConductorMode mode) {
      // No mode means OFFLINE
      if (mode == null) {
        mode = RingGroupConductorMode.OFFLINE;
      }
      doNotify(new RingGroupConductorModeNotification(ringGroup, mode));
    }
  }

  private class DomainGroupMetadataMonitor implements DomainGroupListener {

    @Override
    public void onDomainGroupChange(DomainGroup domainGroup) {
      doNotify(new DomainGroupChangeNotification(ringGroup));
    }
  }

  public RingGroupMonitor(RingGroup ringGroup,
                          List<Notifier> notifiers) throws IOException {
    this.notifiers = notifiers;
    this.ringGroup = ringGroup;
    for (Ring ring : ringGroup.getRings()) {
      ringMonitors.add(new RingMonitor(ringGroup, ring, notifiers));
    }
    this.ringGroupConductorStatusMonitor = new RingGroupConductorModeMonitor();
    this.domainGroupMetadataMonitor = new DomainGroupMetadataMonitor();

    ringGroup.addRingGroupConductorModeListener(ringGroupConductorStatusMonitor);
    ringGroup.getDomainGroup().addListener(domainGroupMetadataMonitor);
  }

  private void doNotify(Notification notification) {
    for (Notifier notifier : notifiers) {
      notifier.doNotify(notification);
    }
  }

  public void stop() {
    for (RingMonitor ringMonitor : ringMonitors) {
      ringMonitor.stop();
    }
    ringGroup.removeRingGroupConductorModeListener(ringGroupConductorStatusMonitor);
    ringGroup.getDomainGroup().removeListener(domainGroupMetadataMonitor);
  }
}
