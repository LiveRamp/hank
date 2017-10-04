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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import com.liveramp.hank.config.InvalidConfigurationException;

public abstract class YamlConfigurator implements Serializable {

  protected Map<String, Object> config;
  private String contentSource;

  public YamlConfigurator() {
  }

  public YamlConfigurator(String configurationFilePath) throws FileNotFoundException,
      InvalidConfigurationException {
    loadFromFile(configurationFilePath);
    validate();
  }

  protected abstract void validate() throws InvalidConfigurationException;

  public void loadFromFile(String path) throws InvalidConfigurationException, FileNotFoundException {
    try {
      contentSource = path;
      config = (Map<String, Object>)new Yaml().load(new BufferedInputStream(new FileInputStream(path)));
    } catch (Exception e) {
      throw new RuntimeException("Invalid configuration in file " + path, e);
    }
    validate();
  }

  public void loadFromYaml(String yaml) throws InvalidConfigurationException {
    try {
      contentSource = yaml;
      config = (Map<String, Object>)new Yaml().load(yaml);
    } catch (Exception e) {
      throw new RuntimeException("Invalid configuration: " + yaml, e);
    }
    validate();
  }

  public void loadFromObjectMap(Map<String, Object> yaml) throws InvalidConfigurationException {
    config = yaml;
    validate();
  }

  public String toYaml() {
    return new Yaml().dump(config);
  }

  @Override
  public String toString() {
    return toYaml();
  }

  protected void checkNonEmptyConfiguration() throws InvalidConfigurationException {
    if (config == null) {
      throw new InvalidConfigurationException("Configuration is empty '" + contentSource + "'");
    }
  }

  protected Object getRequiredOption(String... optionPath) throws InvalidConfigurationException {
    Map<String, Object> currentSection = config;
    String path = "_root";
    int i = 0;
    for (; i < optionPath.length - 1; ++i) {
      if (currentSection.get(optionPath[i]) == null) {
        throw new InvalidConfigurationException("Section '" + optionPath[i]
            + "' is required in configuration section '" + path + "' of configuration '" + contentSource + "'");
      }
      path = path + ":" + optionPath[i];
      currentSection = (Map<String, Object>)currentSection.get(optionPath[i]);
    }
    if (!currentSection.containsKey(optionPath[i])) {
      throw new InvalidConfigurationException("Option '" + optionPath[i]
          + "' is required in configuration section '" + path + "' of configuration '" + contentSource + "'");
    }
    return currentSection.get(optionPath[i]);
  }

  protected Map<String, Object> getRequiredSection(String... optionPath) throws InvalidConfigurationException {
    Object option = getRequiredOption(optionPath);
    if (!(option instanceof Map)) {
      throw new InvalidConfigurationException("Option '" + Arrays.toString(optionPath) + "' must be of type Map in configuration '" + contentSource + "'");
    }
    return (Map<String, Object>)option;
  }

  protected Map<String, Object> getSection(String... optionPath) {
    try {
      return getRequiredSection(optionPath);
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  protected String getRequiredString(String... optionPath) throws InvalidConfigurationException {
    Object option = getRequiredOption(optionPath);
    if (option != null && !(option instanceof String)) {
      throw new InvalidConfigurationException("Option '" + Arrays.toString(optionPath) + "' must be of type String in configuration '" + contentSource + "'");
    }
    return (String)option;
  }

  protected String getString(String... optionPath) {
    try {
      return getRequiredString(optionPath);
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  protected String getOptionalString(String... optionPath) {
    try {
      return getRequiredString(optionPath);
    } catch (InvalidConfigurationException e) {
      return null;
    }
  }

  protected Object getOptionalObject(String... optionPath){
    try {
      return getRequiredOption(optionPath);
    } catch (InvalidConfigurationException e) {
      return null;
    }
  }


  protected Double getRequiredDouble(String... optionPath) throws InvalidConfigurationException {
    Object option = getRequiredOption(optionPath);
    if (option != null && !(option instanceof Double)) {
      throw new InvalidConfigurationException("Option '" + Arrays.toString(optionPath) + "' must be of type Double in configuration '" + contentSource + "'");
    }
    return (Double)option;
  }

  protected Integer getRequiredInteger(String... optionPath) throws InvalidConfigurationException {
    Object option = getRequiredOption(optionPath);
    if (option != null && !(option instanceof Integer)) {
      throw new InvalidConfigurationException("Option '" + Arrays.toString(optionPath) + "' must be of type Integer in configuration '" + contentSource + "'");
    }
    return (Integer)option;
  }

  protected Integer getInteger(String... optionPath) {
    try {
      return getRequiredInteger(optionPath);
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  protected Integer getOptionalInteger(String... optionPath) {
    try {
      return getRequiredInteger(optionPath);
    } catch (InvalidConfigurationException e) {
      return null;
    }
  }

  protected Double getOptionalDouble(String... optionPath) {
    try {
      return getRequiredDouble(optionPath);
    } catch (InvalidConfigurationException e) {
      return null;
    }
  }

  protected Long getRequiredLong(String... optionPath) throws InvalidConfigurationException {
    Object option = getRequiredOption(optionPath);
    // Translate integers to longs
    if (option instanceof Integer) {
      option = Long.valueOf((Integer)option);
    }
    if (option != null && !(option instanceof Long)) {
      throw new InvalidConfigurationException("Option '" + Arrays.toString(optionPath) + "' must be of type Long in configuration '" + contentSource + "'");
    }
    return (Long)option;
  }

  protected Long getLong(String... optionPath) {
    try {
      return getRequiredLong(optionPath);
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  protected Long getOptionalLong(String... optionPath) {
    try {
      return getRequiredLong(optionPath);
    } catch (InvalidConfigurationException e) {
      return null;
    }
  }

  protected List<String> getRequiredStringList(String... optionPath) throws InvalidConfigurationException {
    Object option = getRequiredOption(optionPath);
    if (option != null && !(option instanceof List)) {
      throw new InvalidConfigurationException("Option '" + Arrays.toString(optionPath) + "' must be of type List of strings in configuration '" + contentSource + "'");
    }
    try {
      return (List<String>)option;
    } catch (ClassCastException e) {
      throw new InvalidConfigurationException("Option '" + Arrays.toString(optionPath) + "' must be of type List of strings in configuration '" + contentSource + "'");
    }
  }

  protected List<String> getOptionalStringList(String... optionPath) {
    try {
      return getRequiredStringList(optionPath);
    } catch (InvalidConfigurationException e) {
      return Collections.emptyList();
    }
  }

  protected List<String> getStringList(String... optionPath) {
    try {
      return getRequiredStringList(optionPath);
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

}
