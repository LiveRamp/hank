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

import com.rapleaf.hank.coordinator.DomainGroupVersion;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.monitor.notifier.Notification;
import com.rapleaf.hank.ui.UiUtils;
import com.rapleaf.hank.ui.controllers.RingGroupController;

import java.io.IOException;

public class RingGroupTargetVersionNotification extends AbstractNotification implements Notification {

  private final RingGroup ringGroup;
  private final Integer targetVersion;

  public RingGroupTargetVersionNotification(RingGroup ringGroup, Integer targetVersion) {
    this.ringGroup = ringGroup;
    this.targetVersion = targetVersion;
  }

  public RingGroup getRingGroup() {
    return ringGroup;
  }

  public DomainGroupVersion getTargetVersion() throws IOException {
    return ringGroup.getDomainGroup().getVersion(targetVersion);
  }

  @Override
  protected String formatCore(NotificationFormatter formatter) {
    String versionInfo;
    try {
      versionInfo = UiUtils.formatDomainGroupVersionInfo(getTargetVersion());
    } catch (IOException e) {
      versionInfo = "unknown";
    }
    return "Target version for "
        + formatter.getWebUiLink(RingGroupController.getRingGroupUrl(ringGroup),
        ringGroup.getName()) + " is " + versionInfo;
  }
}
