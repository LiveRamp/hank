package com.rapleaf.hank.monitor.notifier.db;

import com.rapleaf.hank.config.InvalidConfigurationException;
import org.apache.log4j.Logger;

import java.util.Map;

public class SQLConfigurationFactory extends AbstracSQLNotifierConfigurationFactory implements ISQLNotifierConfigurationFactory {
  private static Logger LOG = Logger.getLogger(SQLConfigurationFactory.class);

  private static final String DRIVER = "driver";
  private static final String CONNECTION_URL = "connection_url";
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";
  private static final String TARGET_VERSION_NOTIFICATIONS_TABLE = "target_version_notifications_table";

  @Override
  public void validate(Map<String, Object> configuration) throws InvalidConfigurationException {
    getRequiredString(configuration, DRIVER);
    getRequiredString(configuration, CONNECTION_URL);
    getRequiredString(configuration, USERNAME);
    getRequiredString(configuration, PASSWORD);
    getRequiredString(configuration, TARGET_VERSION_NOTIFICATIONS_TABLE);
  }

  @Override
  public ISQLNotifierConfiguration createNotifierConfiguration(Map<String, Object> configuration) {
    String driver = getString(configuration, DRIVER);
    String connectionUrl = getString(configuration, CONNECTION_URL);
    String username = getString(configuration, USERNAME);
    String password = getString(configuration, PASSWORD);
    String targetVersionNotificationsTable = getString(configuration, TARGET_VERSION_NOTIFICATIONS_TABLE);

    return new SQLNotifierConfiguration(driver, connectionUrl, username, password, targetVersionNotificationsTable);
  }
}
