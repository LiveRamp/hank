package com.rapleaf.hank.monitor.notifier.db;

import com.rapleaf.hank.monitor.notification.RingGroupTargetVersionNotification;
import com.rapleaf.hank.monitor.notifier.Notification;
import com.rapleaf.hank.monitor.notifier.Notifier;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class SQLNotifier implements Notifier {
  private static Logger LOG = Logger.getLogger(SQLNotifier.class);

  private Connection connection;
  private ISQLNotifierConfiguration configuration;

  public SQLNotifier(ISQLNotifierConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public void doNotify(Notification notification) {
    try {
      if (notification instanceof RingGroupTargetVersionNotification) {
        RingGroupTargetVersionNotification versionNotification = (RingGroupTargetVersionNotification) notification;
        int versionNumber = -1;
        try {
          versionNumber = versionNotification.getTargetVersion().getVersionNumber();
        } catch (IOException e) {
          LOG.warn("Cannot get target version", e);
        }
        PreparedStatement preparedStatement = getConnection().prepareStatement("insert into " + configuration.getTargetVersionTable() + " values (default, ?, ?, ?)");
        preparedStatement.setTimestamp(1, new Timestamp(notification.getDate().getTime()));
        preparedStatement.setString(2, versionNotification.getRingGroup().getName());
        preparedStatement.setInt(3, versionNumber);
        preparedStatement.executeUpdate();
      }
    } catch (SQLException e) {
      LOG.warn("Error with mysql operation", e);
    }
  }

  @Override
  public void stop() {
    try {
      connection.close();
    } catch (SQLException e) {
      LOG.warn("Cannot close database", e);
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
