package com.rapleaf.hank.monitor.notifier.db;

import com.rapleaf.hank.monitor.notifier.Notification;
import com.rapleaf.hank.monitor.notifier.Notifier;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SQLNotifier implements Notifier {
  private static Logger LOG = Logger.getLogger(SQLNotifier.class);

  private Connection connection;
  private ISQLNotifierConfiguration configuration;

  public SQLNotifier(ISQLNotifierConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public void doNotify(Notification notification) {
  }

  @Override
  public void stop() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        LOG.warn("Cannot close database", e);
      }
    }
  }

  private Connection getConnection() {
    if (connection == null) {
      try {
        Class.forName(configuration.getDriver());
        connection = DriverManager.getConnection(configuration.getConnectionUrl(), configuration.getUsername(), configuration.getPassword());
      } catch (ClassNotFoundException e) {
        LOG.warn("Cannot find SQL driver " + configuration.getDriver(), e);
      } catch (SQLException e) {
        LOG.warn("Cannot get SQL connection", e);
      }
    }
    return connection;

  }
}
