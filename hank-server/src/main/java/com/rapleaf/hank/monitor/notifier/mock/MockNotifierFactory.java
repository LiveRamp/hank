package com.rapleaf.hank.monitor.notifier.mock;

import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.monitor.notifier.Notifier;
import com.rapleaf.hank.monitor.notifier.NotifierFactory;

import java.util.Map;

public class MockNotifierFactory implements NotifierFactory {

  @Override
  public void validate(Map<String, Object> configuration) throws InvalidConfigurationException {

  }

  @Override
  public Notifier createNotifier(Map<String, Object> configuration,
                                 String name,
                                 String webUiUrl) {
    return new MockNotifier(configuration);
  }
}
