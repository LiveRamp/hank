package com.rapleaf.hank.coordinator;


/**
 * Used to receive notifications when a particular daemon's state has changed.
 */
public interface DaemonStateChangeListener {
  /**
   * This method is called when the <code>DaemonState</code> for a daemon has
   * changed. The first four parameters in the method are used to identify
   * which daemon's state has changed, so that one
   * <code>DaemonStateChangeListener</code> can listen to muliple daemons.
   * 
   * @param ringGroupName
   * @param ringNumber
   * @param hostName
   * @param type
   * @param newState
   *          the new state of the specified daemon.
   */
  public void onDaemonStateChange(String ringGroupName, int ringNumber, PartDaemonAddress hostName, DaemonType type, DaemonState newState);
}