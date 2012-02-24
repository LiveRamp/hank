package com.rapleaf.hank.monitor.notifier.db;

import com.rapleaf.hank.config.InvalidConfigurationException;
import org.apache.log4j.Logger;

import java.util.Map;

public class AbstracSQLNotifierConfigurationFactory {
  private static Logger LOG = Logger.getLogger(AbstracSQLNotifierConfigurationFactory.class);

  // TODO: factorize this code, also in AbstractNotifierFactory
  protected Object getRequiredConfigurationItem(Map<String, Object> configuration,
                                                String key) throws InvalidConfigurationException {
    if (!configuration.containsKey(key)) {
      throw new InvalidConfigurationException(
          "Required notifier factory configuration item '" + key + "' was not found.");
    }
    return configuration.get(key);
  }

  protected String getRequiredString(Map<String, Object> configuration,
                                     String key) throws InvalidConfigurationException {
    return (String) getRequiredConfigurationItem(configuration, key);
  }

  protected String getString(Map<String, Object> configuration, String key) {
    try {
      return getRequiredString(configuration, key);
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException(e);
    }
  }
}
