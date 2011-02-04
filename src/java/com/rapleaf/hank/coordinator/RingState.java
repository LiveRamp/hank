package com.rapleaf.hank.coordinator;

import com.rapleaf.hank.config.RingConfigImpl;
import com.rapleaf.hank.coordinator.zk.ZooKeeperCoordinator;

/**
 * Reflects the status of a ring, and takes on values of
 * <code>AVAILABLE</code>, <code>UPDATING</code>, and <code>DOWN</code>.
 * <code>RingState</code> is a function of the states of the hosts in the
 * ring. If all the hosts are serving, then the ring is <code>AVAILABLE</code>
 * . If any host is down or updating, then the ring will be <code>DOWN</code>
 * or <code>UPDATING</code>, respectively. In the case that there are both
 * down hosts and updating hosts, the ring will be <code>UPDATING</code>,
 * because updating implies that the ring is down, but not vice versa.
 * 
 * @see {@link RingConfigImpl#loadRingStateFromZooKeeper(org.apache.zookeeper.ZooKeeper, ZooKeeperCoordinator, String, int)
 */
public enum RingState {
  // Please do not change the names associated with the RingStates unless you know what you are doing!
  AVAILABLE("available"),
  UPDATING("updating"),
  DOWN("down");

  public final String name;

  RingState(String name) {
    this.name = name;
  }
}