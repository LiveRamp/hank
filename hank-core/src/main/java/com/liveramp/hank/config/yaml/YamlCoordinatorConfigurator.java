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

import com.liveramp.hank.config.CoordinatorConfigurator;
import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.CoordinatorFactory;

import java.io.FileNotFoundException;
import java.io.Serializable;

public class YamlCoordinatorConfigurator extends YamlConfigurator
    implements CoordinatorConfigurator, Serializable {

  private static final String COORDINATOR_SECTION_KEY = "coordinator";
  private static final String COORDINATOR__FACTORY_KEY = "factory";
  private static final String COORDINATOR__OPTIONS_KEY = "options";

  public YamlCoordinatorConfigurator() {
  }

  public YamlCoordinatorConfigurator(String configPath) throws FileNotFoundException,
      InvalidConfigurationException {
    super(configPath);
  }

  @Override
  protected void validate() throws InvalidConfigurationException {
    checkNonEmptyConfiguration();
    getRequiredString(COORDINATOR_SECTION_KEY, COORDINATOR__FACTORY_KEY);
    getRequiredSection(COORDINATOR_SECTION_KEY, COORDINATOR__OPTIONS_KEY);
  }

  @Override
  public Coordinator createCoordinator() {
    try {
      validate();
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException("Configuration is invalid!", e);
    }
    String factoryClassName = getString(COORDINATOR_SECTION_KEY, COORDINATOR__FACTORY_KEY);
    Class<CoordinatorFactory> factoryClass;
    try {
      factoryClass = (Class<CoordinatorFactory>) Class.forName(factoryClassName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not load coordinator factory class " + factoryClassName + "!", e);
    }
    CoordinatorFactory factory;
    try {
      factory = factoryClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Could not get an instance of " + factoryClass.getName() + "!", e);
    }
    return factory.getCoordinator(getSection(COORDINATOR_SECTION_KEY, COORDINATOR__OPTIONS_KEY));
  }
}
