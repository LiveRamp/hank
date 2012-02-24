package com.rapleaf.hank.monitor.notifier.db;

import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.monitor.notifier.AbstractNotifierFactory;
import com.rapleaf.hank.monitor.notifier.NotifierFactory;
import org.apache.log4j.Logger;

import java.util.Map;

public class SQLNotifierFactory extends AbstractNotifierFactory implements NotifierFactory {
  private static Logger LOG = Logger.getLogger(SQLNotifierFactory.class);

  private static final String SQL_CONFIGURATION_FACTORY = "sql_configuration_factory";
  private static final String SQL_CONFIGURATION = "sql_configuration";

  @Override
  public void validate(Map<String, Object> configuration) throws InvalidConfigurationException {
    getRequiredString(configuration, SQL_CONFIGURATION_FACTORY);
    getRequiredString(configuration, SQL_CONFIGURATION);
  }

  @Override
  public SQLNotifier createNotifier(Map<String, Object> configuration, String name, String webUiUrl) {
    String factoryClassName = getString(configuration, SQL_CONFIGURATION_FACTORY);
    ISQLNotifierConfigurationFactory configurationFactory = getConfigurationFactory(factoryClassName);

    try {
      if (!(configuration.get(SQL_CONFIGURATION) instanceof Map)) {
        throw new InvalidConfigurationException("Invalid configuration for SQL notifier");
      }
      configuration = (Map<String, Object>) configuration.get(SQL_CONFIGURATION);
      configurationFactory.validate(configuration);
      ISQLNotifierConfiguration notifierConfiguration = configurationFactory.createNotifierConfiguration(configuration);
      if (notifierConfiguration == null) {
        throw new InvalidConfigurationException("Invalid configuration for SQL notifier");
      }
      return new SQLNotifier(notifierConfiguration);
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException("Failed to create sql notifier", e);
    }
  }

  private ISQLNotifierConfigurationFactory getConfigurationFactory(String factoryClassName) {
    Class configurationFactoryClass;
    ISQLNotifierConfigurationFactory configurationFactory;
    try {
      configurationFactoryClass = Class.forName(factoryClassName);
      configurationFactory = (ISQLNotifierConfigurationFactory) configurationFactoryClass.newInstance();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to find notifier configuration factory class: " + factoryClassName, e);
    } catch (InstantiationException e) {
      throw new RuntimeException("Failed to instantiate notifier configuration factory.", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Failed to instantiate notifier configuration factory.", e);
    }
    return configurationFactory;
  }
}
