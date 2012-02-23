package com.rapleaf.hank.monitor.notifier;

import com.rapleaf.hank.monitor.notification.SQLNotificationFormatter;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class SQLNotifier implements Notifier {
  private static Logger LOG = Logger.getLogger(SQLNotifier.class);

  private Connection connect;
  private SQLNotificationFormatter formatter;
  private String table;

  public SQLNotifier(String driver, String url, String table) {
    this.table = table;
    try {
      Class.forName(driver);
      connect = DriverManager.getConnection(url);
    } catch (ClassNotFoundException e) {
      LOG.warn("Cannot find SQL driver " + driver, e);
    } catch (SQLException e) {
      LOG.warn("Cannot get SQL connection", e);
    }
  }

  @Override
  public void doNotify(Notification notification) {
    try {
      PreparedStatement preparedStatement = connect.prepareStatement("insert into " + table + " values (default, ?, ?)");
      preparedStatement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
      System.out.println(notification.format(formatter));
      preparedStatement.setString(2, notification.format(formatter));
      preparedStatement.executeUpdate();
    } catch (SQLException e) {
      LOG.warn("Error with mysql operation", e);
    }
  }

  @Override
  public void stop() {
    try {
      connect.close();
    } catch (SQLException e) {
      LOG.warn("Cannot close database", e);
    }
  }
}
