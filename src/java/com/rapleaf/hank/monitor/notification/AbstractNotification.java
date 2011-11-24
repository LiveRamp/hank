package com.rapleaf.hank.monitor.notification;

import com.rapleaf.hank.monitor.notifier.Notification;

import java.text.DateFormat;
import java.util.Date;

public abstract class AbstractNotification implements Notification {

  private final Date date;
  private static final DateFormat dateFormat = DateFormat.getInstance();

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
