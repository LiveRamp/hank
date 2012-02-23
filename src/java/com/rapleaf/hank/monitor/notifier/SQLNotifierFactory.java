package com.rapleaf.hank.monitor.notifier;

import com.rapleaf.hank.config.InvalidConfigurationException;
import org.apache.log4j.Logger;

import java.util.Map;

public class SQLNotifierFactory extends AbstractNotifierFactory implements NotifierFactory {
  private static Logger LOG = Logger.getLogger(SQLNotifierFactory.class);

  @Override
  public void validate(Map<String, Object> configuration) throws InvalidConfigurationException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Notifier createNotifier(Map<String, Object> configuration, String name, String webUiUrl) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
