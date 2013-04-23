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

package com.liveramp.hank.config.yaml;

import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.MonitorConfigurator;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.monitor.notifier.Notifier;
import com.liveramp.hank.monitor.notifier.NotifierFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class YamlMonitorConfigurator extends YamlConfigurator implements MonitorConfigurator {

  private static final String MONITOR_SECTION_KEY = "monitor";
  private static final String WEB_UI_URL_KEY = "web_ui_url";
  private static final String NOTIFIERS_CONFIGURATIONS_SECTION_KEY = "notifier_configurations";
  private static final String GLOBAL_NOTIFIER_CONFIGURATIONS_KEY = "global_notifier_configurations";
  private static final String RING_GROUP_NOTIFIERS_CONFIGURATIONS_SECTION_KEY = "ring_group_notifier_configurations";
  private static final String NOTIFIER_FACTORY_CLASS_KEY = "factory";
  private static final String NOTIFIER_CONFIGURATION_SECTION_KEY = "configuration";


  private final Map<String, NotifierFactory> notifierConfigurationNameToNotifierFactory = new HashMap<String, NotifierFactory>();
  private final Map<RingGroup, List<Notifier>> ringGroupToNotifiers = new HashMap<RingGroup, List<Notifier>>();
  private List<Notifier> globalNotifiers;

  public YamlMonitorConfigurator(String configurationPath) throws FileNotFoundException, InvalidConfigurationException {
    super(configurationPath);
  }

  @Override
  protected void validate() throws InvalidConfigurationException {
    getRequiredSection(MONITOR_SECTION_KEY);
    getRequiredString(MONITOR_SECTION_KEY, WEB_UI_URL_KEY);
    getRequiredSection(MONITOR_SECTION_KEY, NOTIFIERS_CONFIGURATIONS_SECTION_KEY);
    getRequiredStringList(MONITOR_SECTION_KEY, GLOBAL_NOTIFIER_CONFIGURATIONS_KEY);
    getRequiredSection(MONITOR_SECTION_KEY, RING_GROUP_NOTIFIERS_CONFIGURATIONS_SECTION_KEY);
  }

  @Override
  public List<Notifier> getGlobalNotifiers() throws InvalidConfigurationException {
    if (globalNotifiers == null) {
      List<String> notifierConfigurationNames;
      notifierConfigurationNames = getRequiredStringList(MONITOR_SECTION_KEY,
          GLOBAL_NOTIFIER_CONFIGURATIONS_KEY);
      globalNotifiers = createNotifiers(notifierConfigurationNames, "Monitor");
    }
    return globalNotifiers;

  }

  @Override
  public List<Notifier> getRingGroupNotifiers(RingGroup ringGroup) throws InvalidConfigurationException {
    List<Notifier> notifiers = ringGroupToNotifiers.get(ringGroup);
    if (notifiers == null) {
      List<String> notifierConfigurationNames;
      notifierConfigurationNames = getRequiredStringList(MONITOR_SECTION_KEY,
          RING_GROUP_NOTIFIERS_CONFIGURATIONS_SECTION_KEY, ringGroup.getName());
      notifiers = createNotifiers(notifierConfigurationNames, ringGroup.getName());
      ringGroupToNotifiers.put(ringGroup, notifiers);
    }
    return notifiers;
  }

  private List<Notifier> createNotifiers(List<String> notifierConfigurationNames, String notifierName) throws InvalidConfigurationException {
    List<Notifier> notifiers = new ArrayList<Notifier>();
    for (String notifierConfigurationName : notifierConfigurationNames) {
      NotifierFactory notifierFactory = getNotifierFactory(notifierConfigurationName);
      // Get configuration
      Map<String, Object> configuration = getRequiredSection(MONITOR_SECTION_KEY,
          NOTIFIERS_CONFIGURATIONS_SECTION_KEY,
          notifierConfigurationName, NOTIFIER_CONFIGURATION_SECTION_KEY);
      notifierFactory.validate(configuration);
      // Create notifier
      Notifier notifier = notifierFactory.createNotifier(configuration, notifierName,
          getString(MONITOR_SECTION_KEY, WEB_UI_URL_KEY));
      notifiers.add(notifier);
    }
    return notifiers;
  }

  private NotifierFactory getNotifierFactory(String notifierConfigurationName) throws InvalidConfigurationException {
    NotifierFactory notifierFactory = notifierConfigurationNameToNotifierFactory.get(notifierConfigurationName);
    if (notifierFactory == null) {
      notifierFactory = createNotifierFactory(notifierConfigurationName);
      notifierConfigurationNameToNotifierFactory.put(notifierConfigurationName, notifierFactory);
    }
    return notifierFactory;
  }

  private NotifierFactory createNotifierFactory(String notifierConfigurationName) throws InvalidConfigurationException {
    String notifierFactoryClassName = getRequiredString(MONITOR_SECTION_KEY, NOTIFIERS_CONFIGURATIONS_SECTION_KEY,
        notifierConfigurationName, NOTIFIER_FACTORY_CLASS_KEY);
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
