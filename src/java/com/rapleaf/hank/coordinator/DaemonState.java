package com.rapleaf.hank.coordinator;

import com.rapleaf.hank.util.Bytes;

/**
 * Used to denote the state of part daemons (<code>DaemonType.PART_DAEMON</code>) and update daemons
 * (<code>DaemonType.UPDATE_DAEMON</code>).
 * 
 * When a third-party, such as the data deployer, wishes to start or stop the part server, it will
 * set the part daemon state to <code>DaemonState.STARTABLE</code> or <code>DaemonState.STOPPABLE</code>,
 * respectively. Upon receipt of the message, the part daemon acknowledges the message by setting its own
 * state to <code>DaemonState.STARTING</code> or <DaemonState.STOPPING</code>, respectively. When it has finally
 * started or stopped, it will set its state to <code>DaemonState.STARTED</code> or <code>DaemonState.IDLE</code>.
 * The process is analogous for the update daemon.
 */
public enum DaemonState {
  // WARNING: Please do not change the id or name of the DaemonStates unless
  // you know what you are doing!
  // Used by the part daemon (DaemonType.PART_DAEMON)
  STARTABLE,
  STARTING,
  STARTED,
  STOPPABLE,
  STOPPING,

  // Used by the update daemon (DaemonType.UPDATE_DAEMON)
  UPDATEABLE,
  UPDATING,

  // Used by both the part daemon and the update daemon
  IDLE,

  UNDEFINED;

  DaemonState() {}

  public static DaemonState byBytes(byte[] bytes) {
    return DaemonState.valueOf(Bytes.bytesToString(bytes));
  }
}