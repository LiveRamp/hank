package com.rapleaf.hank.coordinator;

import java.util.HashMap;
import java.util.Map;

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
  STARTABLE("startable", 1),
  STARTING("starting", 2),
  STARTED("started", 3),
  STOPPABLE("stoppable", 4),
  STOPPING("stopping", 5),

  // Used by the update daemon (DaemonType.UPDATE_DAEMON)
  UPDATEABLE("updateable", 6),
  UPDATING("updating", 7),

  // Used by both the part daemon and the update daemon
  IDLE("idle", 8),

  UNDEFINED("undefined", -1);

  private static final Map<Integer, DaemonState> idMap = new HashMap<Integer, DaemonState>();
  private static Map<String, DaemonState> nameMap = new HashMap<String, DaemonState>();

  static {
    for (DaemonState state : DaemonState.values()) {
      idMap.put(state.id, state);
      nameMap.put(state.name, state);
    }
  }

  public final String name;
  public final int id;

  DaemonState(String name, int id) {
    this.name = name;
    this.id = id;
  }

  public static DaemonState byName(String name) {
    DaemonState state = nameMap.get(name);
    return state != null ? state : UNDEFINED;
  }

  public static DaemonState byId(int id) {
    DaemonState state = idMap.get(id);
    return state != null ? state : UNDEFINED;
  }

  public static DaemonState byBytes(byte[] bytes) {
    return DaemonState.byName(Bytes.bytesToString(bytes));
  }
}