package com.liveramp.hank.monitor.notifier.db;

import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.test.BaseTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class SQLEnvironmentsNotifierFactoryTest extends BaseTestCase {

  private static Map<String, Object> workingConfiguration = new HashMap<String, Object>() {{
    put(SQLEnvironmentsNotifierFactory.DRIVER, "com.mysql.jdbc.Driver");
    put(SQLEnvironmentsNotifierFactory.DATABASE_ENVIRONMENT, "testdb");
    put(SQLEnvironmentsNotifierFactory.ENVIRONMENT_CONFIG_FILE, "config/environment.yml");
    put(SQLEnvironmentsNotifierFactory.DATABASES_CONFIG_FILE, "config/database.yml");
    put(SQLEnvironmentsNotifierFactory.TARGET_VERSION_NOTIFICATIONS_TABLE, "hank_target_version_notifications");
  }};

  private static Map<String, Object> configurationMissingFile = new HashMap<String, Object>() {{
    put(SQLEnvironmentsNotifierFactory.DRIVER, "com.mysql.jdbc.Driver");
    put(SQLEnvironmentsNotifierFactory.DATABASE_ENVIRONMENT, "testdb");
    put(SQLEnvironmentsNotifierFactory.ENVIRONMENT_CONFIG_FILE, "config/environment2.yml");
    put(SQLEnvironmentsNotifierFactory.DATABASES_CONFIG_FILE, "config/database.yml");
    put(SQLEnvironmentsNotifierFactory.TARGET_VERSION_NOTIFICATIONS_TABLE, "hank_target_version_notifications");
  }};

  private static final String environmentConfig = "testdb: test_db_config";
  private static final String invalidEnvironmentConfig = "testdb2: test_db_config";
  private static final String workingDatabaseConfig =
      "test_db_config:\n" +
          "  " + SQLEnvironmentsNotifierFactory.ADAPTER + ": mysql\n" +
          "  " + SQLEnvironmentsNotifierFactory.HOST + ": localhost\n" +
          "  " + SQLEnvironmentsNotifierFactory.DATABASE + ": test_database\n" +
          "  " + SQLEnvironmentsNotifierFactory.USERNAME + ": test\n" +
          "  " + SQLEnvironmentsNotifierFactory.PASSWORD + ": password\n";
  private static final String databaseConfigMissingField =
      "test_db_config:\n" +
          "  " + SQLEnvironmentsNotifierFactory.ADAPTER + ": mysql\n" +
          "  " + SQLEnvironmentsNotifierFactory.HOST + ": localhost\n" +
          "  " + SQLEnvironmentsNotifierFactory.USERNAME + ": test\n" +
          "  " + SQLEnvironmentsNotifierFactory.PASSWORD + ": password\n";
  private static final String databaseConfigWrongConfigName =
      "test_db_config2:\n" +
          "  " + SQLEnvironmentsNotifierFactory.ADAPTER + ": mysql\n" +
          "  " + SQLEnvironmentsNotifierFactory.HOST + ": localhost\n" +
          "  " + SQLEnvironmentsNotifierFactory.DATABASE + ": test_database\n" +
          "  " + SQLEnvironmentsNotifierFactory.USERNAME + ": test\n" +
          "  " + SQLEnvironmentsNotifierFactory.PASSWORD + ": password\n";

  private SQLEnvironmentsNotifierFactory factory;
  private File configDirectory;
  private File databaseFile;
  private File environmentFile;

  @Before
  public void setUp() throws Exception {
    factory = new SQLEnvironmentsNotifierFactory();

    configDirectory = new File("config");
    databaseFile = new File("config/database.yml");
    environmentFile = new File("config/environment.yml");
    configDirectory.mkdir();
    databaseFile.createNewFile();
    environmentFile.createNewFile();
  }

  @After
  public void tearDown() throws Exception {
    databaseFile.delete();
    environmentFile.delete();
    configDirectory.delete();
  }

  @Test
  public void testValidateSuccess() throws IOException {
    writeTo(environmentFile, environmentConfig);
    writeTo(databaseFile, workingDatabaseConfig);
    try {
      factory.validate(workingConfiguration);
    } catch (InvalidConfigurationException e) {
      assertTrue("This configuration is valid " + e.getMessage(), false);
    }
  }

  @Test
  public void testValidateFailDatabaseConfigMissingField() throws IOException {
    writeTo(environmentFile, environmentConfig);
    writeTo(databaseFile, databaseConfigMissingField);
    try {
      factory.validate(workingConfiguration);
      assertTrue("This configuration is invalid", false);
    } catch (InvalidConfigurationException e) {
      assertTrue("This configuration is invalid " + e.getMessage(), true);
    }
  }

  @Test
  public void testValidateFailWrongDatabaseConfigName() throws IOException {
    writeTo(environmentFile, environmentConfig);
    writeTo(databaseFile, databaseConfigWrongConfigName);
    try {
      factory.validate(workingConfiguration);
      assertTrue("This configuration is invalid", false);
    } catch (InvalidConfigurationException e) {
      assertTrue("This configuration is invalid " + e.getMessage(), true);
    }
  }

  @Test
  public void testValidateFailMissingConfigFile() throws IOException {
    writeTo(environmentFile, environmentConfig);
    writeTo(databaseFile, workingDatabaseConfig);
    try {
      factory.validate(configurationMissingFile);
      assertTrue("This configuration is invalid", false);
    } catch (Exception e) {
      assertTrue("This configuration is invalid " + e.getMessage(), e.getCause() instanceof FileNotFoundException);
    }
  }

  @Test
  public void testValidateFailWrongDatabaseEnvirnment() throws IOException {
    writeTo(environmentFile, invalidEnvironmentConfig);
    writeTo(databaseFile, workingDatabaseConfig);
    try {
      factory.validate(workingConfiguration);
      assertTrue("This configuration is invalid", false);
    } catch (Exception e) {
      assertTrue("This configuration is invalid " + e.getMessage(), true);
    }
  }

  private void writeTo(File file, String config) throws IOException {
    FileWriter fileWriter = new FileWriter(file);
    fileWriter.write(config);
    fileWriter.close();
  }
}
