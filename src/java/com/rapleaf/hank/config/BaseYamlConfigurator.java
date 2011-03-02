package com.rapleaf.hank.config;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.CoordinatorFactory;

public abstract class BaseYamlConfigurator implements Configurator {
  private static final String COORDINATOR_SECTION_KEY = "coordinator";
  private static final String COORDINATOR__FACTORY_KEY = "factory";
  private static final String COORDINATOR__OPTIONS_KEY = "options";

  protected final Map<String, Object> config;

  protected BaseYamlConfigurator(String path) throws IOException, InvalidConfigurationException {
    config = (Map<String, Object>) new Yaml().load(new BufferedInputStream(new FileInputStream(path)));
    if (config == null) {
      throw new InvalidConfigurationException(
          "Failed to load config from file " + path
          + ": load resulted in a null config map!");
    }
    validate();
  }

  protected void validate() throws InvalidConfigurationException {
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
