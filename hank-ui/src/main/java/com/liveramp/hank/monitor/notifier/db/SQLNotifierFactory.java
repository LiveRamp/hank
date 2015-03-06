package com.liveramp.hank.monitor.notifier.db;

import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.monitor.notifier.AbstractNotifierFactory;
import com.liveramp.hank.monitor.notifier.NotifierFactory;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import java.util.Map;

public class SQLNotifierFactory extends AbstractNotifierFactory implements NotifierFactory {
  private static Logger LOG = LoggerFactory.getLogger(SQLNotifierFactory.class);

  protected static final String DRIVER = "driver";
  protected static final String CONNECTION_URL = "connection_url";
  protected static final String USERNAME = "username";
  protected static final String PASSWORD = "password";
  protected static final String TARGET_VERSION_NOTIFICATIONS_TABLE = "target_version_notifications_table";

  @Override
  public void validate(Map<String, Object> configuration) throws InvalidConfigurationException {
    getRequiredString(configuration, DRIVER);
    getRequiredString(configuration, CONNECTION_URL);
    getRequiredString(configuration, USERNAME);
    getRequiredString(configuration, PASSWORD);
    getRequiredString(configuration, TARGET_VERSION_NOTIFICATIONS_TABLE);
  }

  @Override
  public SQLNotifier createNotifier(Map<String, Object> configuration, String name, String webUiUrl) {
    String driver = getString(configuration, DRIVER);
    String connectionUrl = getString(configuration, CONNECTION_URL);
    String username = getString(configuration, USERNAME);
    String password = getString(configuration, PASSWORD);
    String targetVersionNotificationsTable = getString(configuration, TARGET_VERSION_NOTIFICATIONS_TABLE);

    ISQLNotifierConfiguration notifierConfiguration = new SQLNotifierConfiguration(driver, connectionUrl, username, password, targetVersionNotificationsTable);
    return new SQLNotifier(notifierConfiguration);
  }

}
