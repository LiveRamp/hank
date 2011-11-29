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

package com.rapleaf.hank.monitor.notification;

import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.monitor.notifier.Notification;
import com.rapleaf.hank.ring_group_conductor.RingGroupConductorMode;
import com.rapleaf.hank.ui.controllers.RingGroupController;

public class RingGroupConductorModeNotification extends AbstractNotification implements Notification {

  private final RingGroup ringGroup;
  private final RingGroupConductorMode mode;

  @Override
  public int hashCode() {
    int result = ringGroup != null ? ringGroup.hashCode() : 0;
    result = 31 * result + (mode != null ? mode.hashCode() : 0);
    return result;
  }

  public RingGroupConductorModeNotification(RingGroup ringGroup, RingGroupConductorMode mode) {
    this.ringGroup = ringGroup;
    this.mode = mode;
  }

  @Override
  protected String formatCore(NotificationFormatter formatter) {
    return "Ring Group Conductor mode for "
        + formatter.getWebUiLink(RingGroupController.getRingGroupUrl(ringGroup),
        ringGroup.getName()) + " is " + mode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RingGroupConductorModeNotification that = (RingGroupConductorModeNotification) o;

    if (mode != that.mode) return false;
    if (ringGroup != null ? !ringGroup.equals(that.ringGroup) : that.ringGroup != null) return false;

    return true;
  }
}
