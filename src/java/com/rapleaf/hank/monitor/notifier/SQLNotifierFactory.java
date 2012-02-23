package com.rapleaf.hank.monitor.notifier;

import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.monitor.notification.StringNotification;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class SQLNotifierFactory extends AbstractNotifierFactory implements NotifierFactory {
  private static Logger LOG = Logger.getLogger(SQLNotifierFactory.class);

  private static final String SQL_DRIVER = "sql_driver";
  private static final String SQL_CONNECTION_URL = "sql_connection_url";
  private static final String SQL_NOTIFICATION_TABLE = "sql_notification_table";

  @Override
  public void validate(Map<String, Object> configuration) throws InvalidConfigurationException {
    getRequiredString(configuration, SQL_DRIVER);
    getRequiredString(configuration, SQL_CONNECTION_URL);
    getRequiredString(configuration, SQL_NOTIFICATION_TABLE);
  }

  @Override
  public SQLNotifier createNotifier(Map<String, Object> configuration, String name, String webUiUrl) {
    String driver = getString(configuration, SQL_DRIVER);
    String url = getString(configuration, SQL_CONNECTION_URL);
    String table = getString(configuration, SQL_NOTIFICATION_TABLE);

    return new SQLNotifier(driver, url, table);
  }

  public static void main(String arg[]) {
    Map<String, Object> configuration = new HashMap<String, Object>() {{
      put(SQL_DRIVER, "com.mysql.jdbc.Driver");
      put(SQL_CONNECTION_URL, "jdbc:mysql://localhost/maindb_development?user=root&password=");
      put(SQL_NOTIFICATION_TABLE, "hank_notification");
    }};
    String name = "test";
    String url = "url";
    SQLNotifier notifier = new SQLNotifierFactory().createNotifier(configuration, name, url);
    notifier.doNotify(new StringNotification("test2"));
  }

}
