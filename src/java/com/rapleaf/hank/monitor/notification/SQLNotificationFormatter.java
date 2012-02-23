package com.rapleaf.hank.monitor.notification;

import org.apache.log4j.Logger;

public class SQLNotificationFormatter implements NotificationFormatter {
  private static Logger LOG = Logger.getLogger(SQLNotificationFormatter.class);

  @Override
  public String getWebUiLink(String absolutePath, String text) {
    return text;
  }
}
