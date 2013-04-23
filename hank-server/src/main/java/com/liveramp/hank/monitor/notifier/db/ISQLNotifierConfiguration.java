package com.liveramp.hank.monitor.notifier.db;

public interface ISQLNotifierConfiguration {
  String getDriver();

  String getConnectionUrl();

  String getUsername();

  String getPassword();

  String getTargetVersionTable();

}
