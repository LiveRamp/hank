package com.rapleaf.hank.coordinator;

/**
 * <code>DaemonType.PART_DAEMON</code> and <code>DaemonType.UPDATE_DAEMON</code> are used to 
 * differentiate the part daemon and the update daemon running on the same host, respectively.
 */
public enum DaemonType {
  // Please do not change the names associated with the DaemonTypes unless you
  // know what you are doing!
  PART_DAEMON("part_daemon"),
  UPDATE_DAEMON("update_daemon");

  public final String name;

  DaemonType(String name) {
    this.name = name;
  }
}