package com.liveramp.hank.monitor.notifier.db;

import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.test.BaseTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SQLNotifierFactoryTest extends BaseTestCase {

  private static Map<String, Object> workingConfiguration = new HashMap<String, Object>() {{
    put(SQLNotifierFactory.DRIVER, "com.mysql.jdbc.Driver");
    put(SQLNotifierFactory.CONNECTION_URL, "jdbc:mysql://localhost/maindb_development");
    put(SQLNotifierFactory.USERNAME, "test");
    put(SQLNotifierFactory.PASSWORD, "test");
    put(SQLNotifierFactory.TARGET_VERSION_NOTIFICATIONS_TABLE, "hank_target_version_notifications");
  }};

  private static Map<String, Object> nonWorkingConfiguration = new HashMap<String, Object>() {{
    put(SQLNotifierFactory.DRIVER, "com.mysql.jdbc.Driver");
    put(SQLNotifierFactory.CONNECTION_URL, "jdbc:mysql://localhost/maindb_development");
    put(SQLNotifierFactory.USERNAME, "test");
    put(SQLNotifierFactory.TARGET_VERSION_NOTIFICATIONS_TABLE, "hank_target_version_notifications");
  }};


  private SQLNotifierFactory factory;

  @Before
  public void setUp() throws Exception {
    factory = new SQLNotifierFactory();
  }

  @Test
  public void testValidate() {

    try {
      factory.validate(workingConfiguration);
    } catch (InvalidConfigurationException e) {
      assertTrue("This configuration is valid", false);
    }

    try {
      factory.validate(nonWorkingConfiguration);
    } catch (InvalidConfigurationException e) {
      assertTrue("This configuration is invalid", true);
    }

  }

  @Test
  public void testCreateNotifier() throws Exception {
    assertNotNull(factory.createNotifier(workingConfiguration, "test", ""));
    try {
      assertNull(factory.createNotifier(nonWorkingConfiguration, "test", ""));
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof InvalidConfigurationException);
    }
  }
}
