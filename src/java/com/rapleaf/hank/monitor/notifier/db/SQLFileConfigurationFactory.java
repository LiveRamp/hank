package com.rapleaf.hank.monitor.notifier.db;

import com.rapleaf.hank.config.InvalidConfigurationException;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class SQLFileConfigurationFactory extends AbstracSQLNotifierConfigurationFactory implements ISQLNotifierConfigurationFactory {
  private static Logger LOG = Logger.getLogger(SQLFileConfigurationFactory.class);

  private static final String ENVIRONMENT_CONFIG_FILE = "environment_config_file";
  private static final String DATABASES_CONFIG_FILE = "database_config_file";
  private static final String DATABASE_KEY = "database_key";
  private static final String DRIVER = "driver";
  private static final String TARGET_VERSION_NOTIFICATIONS_TABLE = "target_version_notifications_table";

  @Override
  public void validate(Map<String, Object> configuration) throws InvalidConfigurationException {
    getRequiredString(configuration, ENVIRONMENT_CONFIG_FILE);
    getRequiredString(configuration, DATABASES_CONFIG_FILE);
    getRequiredString(configuration, DATABASE_KEY);
    getRequiredString(configuration, DRIVER);
    getRequiredString(configuration, TARGET_VERSION_NOTIFICATIONS_TABLE);
  }

  @Override
  public ISQLNotifierConfiguration createNotifierConfiguration(Map<String, Object> configuration) {
    String environmentConfigFile = getString(configuration, ENVIRONMENT_CONFIG_FILE);
    String databaseConfigFile = getString(configuration, DATABASES_CONFIG_FILE);
    String databaseName = getString(configuration, DATABASE_KEY);
    String driver = getString(configuration, DRIVER);
    String targetVersionNotificationTable = getString(configuration, TARGET_VERSION_NOTIFICATIONS_TABLE);

    try {
      Map<String, Object> envInfo = (Map<String, Object>) new Yaml().load(new FileReader(environmentConfigFile));
      String dbInfoName = (String) envInfo.get(databaseName);
      Map dbInfoContainer = (Map<String, Object>) new Yaml().load(new FileReader(databaseConfigFile));
      Map<String, String> dbInfo = (Map<String, String>) dbInfoContainer.get(dbInfoName);

      String serverUrl = "jdbc:" + dbInfo.get("adapter") + "://" + dbInfo.get("host");
      String database = dbInfo.get("database");
      String connectionUrl = serverUrl + "/" + database;
      String username = dbInfo.get("username");
      String password = dbInfo.get("password");
      return new SQLNotifierConfiguration(driver, connectionUrl, username, password, targetVersionNotificationTable);
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Can't find config file");
    }
  }
}
