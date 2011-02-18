package com.rapleaf.hank.coordinator;

/**
 * The state of a ring as derived from the state of all its hosts.
 */
public enum RingState {
  AVAILABLE,
  STOPPING,
  STARTING,
  IDLE,
  UPDATING
}