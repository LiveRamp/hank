package com.rapleaf.hank.monitor.notifier;

import com.rapleaf.hank.config.InvalidConfigurationException;

import java.util.Map;

public class MockNotifierFactory implements NotifierFactory {

  @Override
  public void validate(Map<String, Object> configuration) throws InvalidConfigurationException {

  }

  @Override
  public Notifier createNotifier(Map<String, Object> configuration, String name) {
    return new MockNotifier(configuration);
  }
}
