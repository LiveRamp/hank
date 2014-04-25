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
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.monitor.notifier.Notifier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RingMonitor {

  private final RingGroup ringGroup;
  private final Ring ring;
  private final List<Notifier> notifiers;

  private final Collection<HostMonitor> hostMonitors = new ArrayList<HostMonitor>();

  public RingMonitor(final RingGroup ringGroup,
                     final Ring ring,
                     final List<Notifier> notifiers) throws IOException {
    this.ringGroup = ringGroup;
    this.ring = ring;
    this.notifiers = notifiers;
    for (Host host : ring.getHosts()) {
      hostMonitors.add(new HostMonitor(ringGroup, ring, host, notifiers));
    }
  }

  public void stop() {
    for (HostMonitor hostMonitor : hostMonitors) {
      hostMonitor.stop();
    }
  }
}
