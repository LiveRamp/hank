package com.rapleaf.hank.coordinator;

import com.rapleaf.hank.config.RingGroupConfig;

/**
 * Used to receive the latest configuration information when a ring group has
 * changed. Currently there are two cases when
 * <code>RingGroupChangeListener</code> might be called: 1. A host's state has
 * changed, and so the <code>RingState</code> of a ring may have changed. 2. A
 * ring has been added or removed.
 */
public interface RingGroupChangeListener {
  /**
   * Called when the configuration information for a ring group has changed.
   * The latest configuration information is supplied in the arguments.
   * 
   * @param newRingGroup
   *          the latest configuration information for a ring group
   */
  public void onRingGroupChange(RingGroupConfig newRingGroup);
}