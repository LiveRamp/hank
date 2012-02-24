package com.rapleaf.hank.monitor.notifier.db;

import com.rapleaf.hank.config.InvalidConfigurationException;

import java.util.Map;

public interface ISQLNotifierConfigurationFactory {
  public void validate(Map<String, Object> configuration) throws InvalidConfigurationException;

  public ISQLNotifierConfiguration createNotifierConfiguration(Map<String, Object> configuration);
}
