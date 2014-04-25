/**
 *  Copyright 2012 LiveRamp
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

package com.liveramp.hank.monitor.notification;

import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.monitor.notifier.Notification;
import com.liveramp.hank.ui.UiUtils;
import com.liveramp.hank.ui.controllers.RingGroupController;

import java.io.IOException;

public class DomainGroupChangeNotification extends AbstractNotification implements Notification {

  private final RingGroup ringGroup;

  public DomainGroupChangeNotification(RingGroup ringGroup) {
    this.ringGroup = ringGroup;
  }

  @Override
  protected String formatCore(NotificationFormatter formatter) {
    try {
      return "Domain Group for "
          + formatter.getWebUiLink(RingGroupController.getRingGroupUrl(ringGroup),
          ringGroup.getName()) + " is now:"
          + UiUtils.formatDomainGroupDomainVersionsTable(ringGroup.getDomainGroup(), "", false);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DomainGroupChangeNotification that = (DomainGroupChangeNotification) o;

    if (ringGroup != null ? !ringGroup.equals(that.ringGroup) : that.ringGroup != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return ringGroup != null ? ringGroup.hashCode() : 0;
  }
}
