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

import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.monitor.notification.RingGroupConductorModeNotification;
import com.rapleaf.hank.monitor.notification.RingGroupTargetVersionNotification;
import com.rapleaf.hank.monitor.notifier.Notifier;
import com.rapleaf.hank.ring_group_conductor.RingGroupConductorMode;
import com.rapleaf.hank.zookeeper.WatchedNodeListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class RingGroupMonitor {

  private final RingGroup ringGroup;
  private final Notifier notifier;
  private Collection<RingMonitor> ringMonitors = new ArrayList<RingMonitor>();
  private final RingGroupConductorModeMonitor ringGroupConductorStatusMonitor;
  private final TargetVersionMonitor targetVersionMonitor;

  private class RingGroupConductorModeMonitor implements WatchedNodeListener<RingGroupConductorMode> {

    @Override
    public void onWatchedNodeChange(RingGroupConductorMode mode) {
      // No mode means OFFLINE
      if (mode == null) {
        mode = RingGroupConductorMode.OFFLINE;
      }
      notifier.notify(new RingGroupConductorModeNotification(ringGroup, mode));
    }
  }

  private class TargetVersionMonitor implements WatchedNodeListener<Integer> {

    @Override
    public void onWatchedNodeChange(Integer targetVersion) {
      notifier.notify(new RingGroupTargetVersionNotification(ringGroup, targetVersion));
    }
  }

  public RingGroupMonitor(RingGroup ringGroup,
                          Notifier notifier) throws IOException {
    this.notifier = notifier;
    this.ringGroup = ringGroup;
    for (Ring ring : ringGroup.getRings()) {
      ringMonitors.add(new RingMonitor(ringGroup, ring, notifier));
    }
    this.ringGroupConductorStatusMonitor = new RingGroupConductorModeMonitor();
    this.targetVersionMonitor = new TargetVersionMonitor();

    ringGroup.addRingGroupConductorModeListener(ringGroupConductorStatusMonitor);
    ringGroup.addTargetVersionListener(targetVersionMonitor);
  }

  public void stop() {
    for (RingMonitor ringMonitor : ringMonitors) {
      ringMonitor.stop();
    }
    ringGroup.removeRingGroupConductorModeListener(ringGroupConductorStatusMonitor);
    ringGroup.removeTargetVersionListener(targetVersionMonitor);
  }
}
