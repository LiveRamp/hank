package com.liveramp.hank.monitor.notification;

import java.util.Date;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.liveramp.hank.monitor.notifier.Notification;

public abstract class AbstractNotification implements Notification {

  private final Date date;
  private static final String DATE_FORMAT = "yy/MM/dd HH:mm:ss";
  private static final DateTimeFormatter dateFormat = DateTimeFormat.forPattern(DATE_FORMAT);

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
    return dateFormat.print(date.getTime()) + " " + formatCore(formatter);
  }
}
