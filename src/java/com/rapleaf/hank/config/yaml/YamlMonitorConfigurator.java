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

package com.rapleaf.hank.config.yaml;

import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.config.MonitorConfigurator;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.monitor.notifier.Notifier;
import com.rapleaf.hank.monitor.notifier.NotifierFactory;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

public class YamlMonitorConfigurator extends BaseYamlConfigurator implements MonitorConfigurator {

  private static final String MONITOR_SECTION_KEY = "monitor";
  private static final String WEB_UI_URL_KEY = "web_ui_url";
  private static final String GLOBAL_NOTIFIER_FACTORY_CLASS_KEY = "global_notifier_factory";
  private static final String GLOBAL_NOTIFIER_CONFIGURATION_SECTION_KEY = "global_notifier_configuration";

  private static final String RING_GROUP_NOTIFIERS_SECTION_KEY = "ring_group_notifiers";
  private static final String RING_GROUP_NOTIFIER_FACTORY_CLASS_KEY = "factory";
  private static final String RING_GROUP_NOTIFIER_CONFIGURATION_SECTION_KEY = "configuration";

  private final Map<RingGroup, Notifier> notifiers = new HashMap<RingGroup, Notifier>();
  private final Map<RingGroup, NotifierFactory> notifierFactories = new HashMap<RingGroup, NotifierFactory>();
  private Notifier globalNotifier;
  private NotifierFactory globalNotifierFactory;

  public YamlMonitorConfigurator(String configurationPath) throws FileNotFoundException, InvalidConfigurationException {
    super(configurationPath);
  }

  @Override
  protected void validate() throws InvalidConfigurationException {
    getRequiredSection(MONITOR_SECTION_KEY);
    getRequiredString(MONITOR_SECTION_KEY, WEB_UI_URL_KEY);
    getRequiredString(MONITOR_SECTION_KEY, GLOBAL_NOTIFIER_FACTORY_CLASS_KEY);
    getRequiredSection(MONITOR_SECTION_KEY, GLOBAL_NOTIFIER_CONFIGURATION_SECTION_KEY);
    getRequiredSection(MONITOR_SECTION_KEY, RING_GROUP_NOTIFIERS_SECTION_KEY);
  }

  @Override
  public Notifier getGlobalNotifier() throws InvalidConfigurationException {
    if (globalNotifier == null) {
      if (globalNotifierFactory == null) {
        globalNotifierFactory =
            createNotifierFactory(getString(MONITOR_SECTION_KEY, GLOBAL_NOTIFIER_FACTORY_CLASS_KEY));
      }
      Map<String, Object> configuration = getSection(MONITOR_SECTION_KEY, GLOBAL_NOTIFIER_CONFIGURATION_SECTION_KEY);
      globalNotifierFactory.validate(configuration);
      globalNotifier = globalNotifierFactory.createNotifier(configuration, "Monitor",
          getString(MONITOR_SECTION_KEY, WEB_UI_URL_KEY));
    }
    return globalNotifier;
  }

  @Override
  public Notifier getRingGroupNotifier(RingGroup ringGroup) throws InvalidConfigurationException {
    Notifier notifier = notifiers.get(ringGroup);
    if (notifier == null) {
      NotifierFactory notifierFactory = notifierFactories.get(ringGroup);
      if (notifierFactory == null) {
        String notifierClassName = getRequiredString(MONITOR_SECTION_KEY, RING_GROUP_NOTIFIERS_SECTION_KEY,
            ringGroup.getName(), RING_GROUP_NOTIFIER_FACTORY_CLASS_KEY);
        notifierFactory = createNotifierFactory(notifierClassName);
        notifierFactories.put(ringGroup, notifierFactory);
      }
      Map<String, Object> configuration = getRequiredSection(MONITOR_SECTION_KEY, RING_GROUP_NOTIFIERS_SECTION_KEY,
          ringGroup.getName(), RING_GROUP_NOTIFIER_CONFIGURATION_SECTION_KEY);
      notifierFactory.validate(configuration);
      notifier = notifierFactory.createNotifier(configuration, ringGroup.getName(),
          getString(MONITOR_SECTION_KEY, WEB_UI_URL_KEY));
      notifiers.put(ringGroup, notifier);
    }
    return notifier;
  }

  private NotifierFactory createNotifierFactory(String notifierFactoryClassName) {
    Class notifierFactoryClass;
    NotifierFactory notifierFactory;
    try {
      notifierFactoryClass = Class.forName(notifierFactoryClassName);
      notifierFactory = (NotifierFactory) notifierFactoryClass.newInstance();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to find notifier factory class: " + notifierFactoryClassName, e);
    } catch (InstantiationException e) {
      throw new RuntimeException("Failed to instantiate notifier factory.", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Failed to instantiate notifier factory.", e);
    }
    return notifierFactory;
  }
}
