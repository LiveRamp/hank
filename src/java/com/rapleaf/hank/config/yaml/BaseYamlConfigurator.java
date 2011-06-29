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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import com.rapleaf.hank.config.Configurator;
import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.CoordinatorFactory;

public abstract class BaseYamlConfigurator implements Configurator {
  private static final String COORDINATOR_SECTION_KEY = "coordinator";
  private static final String COORDINATOR__FACTORY_KEY = "factory";
  private static final String COORDINATOR__OPTIONS_KEY = "options";

  protected Map<String, Object> config;

  protected BaseYamlConfigurator() {}

  protected BaseYamlConfigurator(String configPath) throws FileNotFoundException, InvalidConfigurationException {
    loadFromFile(configPath);
  }

  public void loadFromFile(String path) throws InvalidConfigurationException, FileNotFoundException {
    try {
      config = (Map<String, Object>) new Yaml().load(new BufferedInputStream(new FileInputStream(path)));
    } catch (Exception e) {
      throw new RuntimeException("Invalid configuration in " + path, e);
    }
    validate();
  }

  public void loadFromYaml(String yaml) throws InvalidConfigurationException {
    try {
      config = (Map<String, Object>) new Yaml().load(yaml);
    } catch (Exception e) {
      throw new RuntimeException("Invalid configuration: " + yaml, e);
    }
    validate();
  }

  protected void validate() throws InvalidConfigurationException {
    if (config == null) {
      throw new InvalidConfigurationException("Configuration is empty!");
    }
    if (config.containsKey(COORDINATOR_SECTION_KEY)) {
      Map<String, Object> coordSection = (Map<String, Object>) config.get(COORDINATOR_SECTION_KEY);
      if (coordSection == null) {
        throw new InvalidConfigurationException("'coordinator' section must not be null!");
      }
      if (!coordSection.containsKey(COORDINATOR__FACTORY_KEY) || !(coordSection.get(COORDINATOR__FACTORY_KEY) instanceof String)) {
        throw new InvalidConfigurationException("'coordinator' section must contain a 'factory' key of type String!");
      }
      if (!coordSection.containsKey(COORDINATOR__OPTIONS_KEY)) {
        throw new InvalidConfigurationException("'coordinator' section must contain an 'options' key!");
      }
    } else {
      throw new InvalidConfigurationException("Configuration must contain a 'coordinator' section!");
    }
  }

  @Override
  public Coordinator getCoordinator() {
    try {
      validate();
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException("Configuration is invalid!", e);
    }
    Map<String, Object> coordSection = (Map<String, Object>) config.get(COORDINATOR_SECTION_KEY);
    String factoryClassName = (String) coordSection.get(COORDINATOR__FACTORY_KEY);
    Class<CoordinatorFactory> factoryClass;
    try {
      factoryClass = (Class<CoordinatorFactory>) Class.forName(factoryClassName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not load coordinator factory class "
          + factoryClassName + "!", e);
    }
    CoordinatorFactory factory;
    try {
      factory = factoryClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Could not get an instance of "
          + factoryClass.getName() + "!", e);
    }
    return factory.getCoordinator((Map<String, Object>) coordSection.get(COORDINATOR__OPTIONS_KEY));
  }
}
