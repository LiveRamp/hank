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

package com.liveramp.hank.monitor.notifier.mock;

import com.liveramp.hank.monitor.notifier.Notification;
import com.liveramp.hank.monitor.notifier.Notifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MockNotifier implements Notifier {

  private final List<Notification> notifications = new ArrayList<Notification>();
  private final Map<String, Object> configuration;

  public MockNotifier() {
    this.configuration = null;
  }

  public MockNotifier(final Map<String, Object> configuration) {
    this.configuration = configuration;
  }

  @Override
  public void doNotify(final Notification notification) {
    notifications.add(notification);
  }

  @Override
  public void stop() {
  }

  public List<Notification> getNotifications() {
    return notifications;
  }

  public Map<String, Object> getConfiguration() {
    return configuration;
  }
}
