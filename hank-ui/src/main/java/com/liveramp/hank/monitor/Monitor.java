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

import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.MonitorConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.monitor.notification.StringNotification;
import com.liveramp.hank.monitor.notifier.Notification;
import com.liveramp.hank.monitor.notifier.Notifier;
import com.liveramp.hank.util.LocalHostUtils;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Monitor {

  private static final Logger LOG = LoggerFactory.getLogger(Monitor.class);

  private final List<Notifier> globalNotifiers;
  private final List<Notifier> ringGroupNotifiers = new ArrayList<Notifier>();
  private final Collection<RingGroupMonitor> ringGroupMonitors = new ArrayList<RingGroupMonitor>();
  private Thread shutdownHook;

  private void notifyGlobalNotifiers(Notification notification) {
    for (Notifier notifier : globalNotifiers) {
      notifier.doNotify(notification);
    }
  }

  public Monitor(Coordinator coordinator,
                 MonitorConfigurator configurator) throws IOException, InvalidConfigurationException {
    globalNotifiers = configurator.getGlobalNotifiers();
    try {
      notifyGlobalNotifiers(new StringNotification("Hank monitor starting on " + LocalHostUtils.getHostName() + "."));
    } catch (UnknownHostException e) {
      notifyGlobalNotifiers(new StringNotification("Hank monitor starting on 'unknown host'."));
    }
    addShutdownHook();

    for (RingGroup ringGroup : coordinator.getRingGroups()) {
      List<Notifier> notifiers;
      try {
        notifiers = configurator.getRingGroupNotifiers(ringGroup);
        ringGroupNotifiers.addAll(notifiers);
        ringGroupMonitors.add(new RingGroupMonitor(ringGroup, notifiers));
      } catch (InvalidConfigurationException e) {
        LOG.error("Invalid configuration for Ring Group " + ringGroup.getName(), e);
        notifyGlobalNotifiers(new StringNotification("Ignoring Ring Group " + ringGroup.getName()
            + " since the corresponding configuration was invalid. It will not be monitored."));
      }
    }
  }

  public void stop() {
    for (RingGroupMonitor ringGroupMonitor : ringGroupMonitors) {
      ringGroupMonitor.stop();
    }
    for (Notifier notifier : globalNotifiers) {
      notifier.stop();
    }
    for (Notifier notifier : ringGroupNotifiers) {
      notifier.stop();
    }
  }

  private void addShutdownHook() {
    removeShutdownHook();
    shutdownHook = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          notifyGlobalNotifiers(new StringNotification("Hank monitor stopping on "
              + LocalHostUtils.getHostName() + "."));
        } catch (UnknownHostException e) {
          notifyGlobalNotifiers(new StringNotification("Hank monitor stopping on 'unknown host'."));
        }
        stop();
      }
    });
    Runtime.getRuntime().addShutdownHook(shutdownHook);
  }

  private void removeShutdownHook() {
    if (shutdownHook != null) {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    }
  }
}
