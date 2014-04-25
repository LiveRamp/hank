package com.liveramp.hank.monitor.notification;

import com.liveramp.hank.monitor.notifier.Notification;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public abstract class AbstractNotification implements Notification {

  private final Date date;
  private static final String DATE_FORMAT = "yy/MM/dd HH:mm:ss";
  private static final DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);

  public AbstractNotification() {
    date = new Date();
  }

  @Override
  public Date getDate() {
    return date;
  }

  protected abstract String formatCore(NotificationFormatter formatter);

  @Override
  public final String format(NotificationFormatter formatter) {
    return dateFormat.format(date) + " " + formatCore(formatter);
  }
}
