package com.liveramp.hank.monitor.notifier.db;

import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.yaml.YamlConfigurator;
import com.liveramp.hank.monitor.notifier.AbstractNotifierFactory;
import com.liveramp.hank.monitor.notifier.NotifierFactory;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.Map;

public class SQLEnvironmentsNotifierFactory extends AbstractNotifierFactory implements NotifierFactory {
  private static Logger LOG = LoggerFactory.getLogger(SQLEnvironmentsNotifierFactory.class);

  protected static final String ENVIRONMENT_CONFIG_FILE = "environment_config_file";
  protected static final String DATABASES_CONFIG_FILE = "database_config_file";
  protected static final String DATABASE_ENVIRONMENT = "database_environment";
  protected static final String DRIVER = "driver";
  protected static final String TARGET_VERSION_NOTIFICATIONS_TABLE = "target_version_notifications_table";

  protected static final String ADAPTER = "adapter";
  protected static final String HOST = "host";
  protected static final String DATABASE = "database";
  protected static final String USERNAME = "username";
  protected static final String PASSWORD = "password";

  private String adapter;
  private String host;
  private String database;
  private String username;
  private String password;
  private SQLEnvironmentsNotifierFactory.EnvironmentYamlConfigurator environmentYamlConfigurator;
  private DatabaseYamlConfigurator databaseYamlConfigurator;

  @Override
  public void validate(Map<String, Object> configuration) throws InvalidConfigurationException {
    String environmentFilename = getRequiredString(configuration, ENVIRONMENT_CONFIG_FILE);
    String databaseFilename = getRequiredString(configuration, DATABASES_CONFIG_FILE);
    String databaseEnvironment = getRequiredString(configuration, DATABASE_ENVIRONMENT);
    getRequiredString(configuration, DRIVER);
    getRequiredString(configuration, TARGET_VERSION_NOTIFICATIONS_TABLE);

    initYamlConfigurators(environmentFilename, databaseFilename, databaseEnvironment);
  }

  private void initYamlConfigurators(String environmentFilename, String databaseFilename, String databaseEnvironment)
      throws InvalidConfigurationException {
    if (environmentYamlConfigurator == null) {
      environmentYamlConfigurator = new EnvironmentYamlConfigurator(environmentFilename, databaseEnvironment);
    }
    if (databaseYamlConfigurator == null) {
      databaseYamlConfigurator = new DatabaseYamlConfigurator(databaseFilename, environmentYamlConfigurator.databaseConfigName);
    }
  }

  @Override
  public SQLNotifier createNotifier(Map<String, Object> configuration, String name, String webUiUrl) {
    String environmentFilename = getString(configuration, ENVIRONMENT_CONFIG_FILE);
    String databaseFilename = getString(configuration, DATABASES_CONFIG_FILE);
    String databaseEnvironment = getString(configuration, DATABASE_ENVIRONMENT);
    String driver = getString(configuration, DRIVER);
    String targetVersionNotificationTable = getString(configuration, TARGET_VERSION_NOTIFICATIONS_TABLE);

    try {
      initYamlConfigurators(environmentFilename, databaseFilename, databaseEnvironment);
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException("Error while reading files", e);
    }

    String serverUrl = "jdbc:" + databaseYamlConfigurator.adapter + "://" + databaseYamlConfigurator.host;
    String database = databaseYamlConfigurator.database;
    String connectionUrl = serverUrl + "/" + database;
    String username = databaseYamlConfigurator.username;
    String password = databaseYamlConfigurator.password;
    SQLNotifierConfiguration notifierNotification = new SQLNotifierConfiguration(driver, connectionUrl, username, password, targetVersionNotificationTable);

    return new SQLNotifier(notifierNotification);
  }

  private class EnvironmentYamlConfigurator extends YamlConfigurator {
    private String databaseEnvironment;

    private String databaseConfigName;

    private EnvironmentYamlConfigurator(String environmentFilename, String databaseEnvironment) throws InvalidConfigurationException {
      this.databaseEnvironment = databaseEnvironment;
      try {
        loadFromFile(environmentFilename);
      } catch (FileNotFoundException e) {
        throw new InvalidConfigurationException("Can't find config file " + e.getMessage());
      }
    }

    @Override
    protected void validate() throws InvalidConfigurationException {
      databaseConfigName = getRequiredString(databaseEnvironment);
    }
  }

  private class DatabaseYamlConfigurator extends YamlConfigurator {
    private String databaseConfigName;

    private String adapter;
    private String host;
    private String database;
    private String username;
    private String password;

    private DatabaseYamlConfigurator(String databaseFilename, String databaseConfigName) throws InvalidConfigurationException {
      this.databaseConfigName = databaseConfigName;
      try {
        loadFromFile(databaseFilename);
      } catch (FileNotFoundException e) {
        throw new InvalidConfigurationException("Can't find config file " + e.getMessage());
      }
    }

    @Override
    protected void validate() throws InvalidConfigurationException {
      getRequiredSection(databaseConfigName);
      adapter = getRequiredString(databaseConfigName, ADAPTER);
      host = getRequiredString(databaseConfigName, HOST);
      database = getRequiredString(databaseConfigName, DATABASE);
      username = getRequiredString(databaseConfigName, USERNAME);
      password = getRequiredString(databaseConfigName, PASSWORD);
    }
  }
}
