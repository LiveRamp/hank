package com.rapleaf.hank.monitor.notification;

import com.rapleaf.hank.monitor.notifier.Notification;

import java.util.Date;

public abstract class AbstractNotification implements Notification {

  private final Date date;

  public AbstractNotification() {
    date = new Date();
  }

  @Override
  public Date getDate() {
    return date;
  }

  protected abstract String formatCore();

  @Override
  public final String format() {
    return date.toString() + " " + formatCore();
  }
}
