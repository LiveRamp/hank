package com.rapleaf.hank.config;

/**
 * Configuration particular to the SmartClient Daemon.
 */
public interface SmartClientDaemonConfigurator extends Configurator {
  public String getRingGroupName();
  public int getPortNumber();
  public int getNumThreads();
}
