package com.rapleaf.hank.config;

public interface SmartClientDaemonConfigurator extends Configurator {
  public String getRingGroupName();
  public int getPortNumber();
  public int getNumThreads();
}
