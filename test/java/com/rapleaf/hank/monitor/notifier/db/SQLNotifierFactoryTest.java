package com.rapleaf.hank.monitor.notifier.db;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.config.InvalidConfigurationException;
import org.apache.log4j.Logger;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

public class SQLNotifierFactoryTest extends BaseTestCase {
  private static Logger LOG = Logger.getLogger(SQLNotifierFactoryTest.class);

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
    super.setUp();
    factory = new SQLNotifierFactory();
  }


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

  public void testCreateNotifier() throws Exception {
    assertNotNull(factory.createNotifier(workingConfiguration, "test", ""));
    try {
      assertNull(factory.createNotifier(nonWorkingConfiguration, "test", ""));
    } catch (Exception e) {
      assertTrue(e.getCause() instanceof InvalidConfigurationException);
    }
  }
}
