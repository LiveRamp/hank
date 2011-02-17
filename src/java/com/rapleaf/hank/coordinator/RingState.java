package com.rapleaf.hank.coordinator;


/**
 * Reflects the status of a ring, and takes on values of
 * <code>AVAILABLE</code>, <code>UPDATING</code>, and <code>DOWN</code>.
 * <code>RingState</code> is a function of the states of the hosts in the
 * ring. If all the hosts are serving, then the ring is <code>AVAILABLE</code>
 * . If any host is down or updating, then the ring will be <code>DOWN</code>
 * or <code>UPDATING</code>, respectively. In the case that there are both
 * down hosts and updating hosts, the ring will be <code>UPDATING</code>,
 * because updating implies that the ring is down, but not vice versa.
 */
public enum RingState {
  // Please do not change the names associated with the RingStates unless you
  // know what you are doing!

  /**
   * A ring is available if there is at least one up-to-date replica of every
   * partition for all domains in the domain group of the ring's ring group.
   */
  AVAILABLE("available"),
  /**
   * A ring is updating if any host in the ring is currently in the process of
   * updating. An updating ring should not be used.
   */
  UPDATING("updating"),
  /**
   * A ring is down if it's not updating, but there isn't a replica for any
   * partition in any domain in the ring's ring group's domain group.
   */
  DOWN("down");

  public final String name;

  RingState(String name) {
    this.name = name;
  }
}