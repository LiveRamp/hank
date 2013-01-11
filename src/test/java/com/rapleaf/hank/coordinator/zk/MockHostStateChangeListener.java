/**
 *
 */
package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.zookeeper.WatchedNodeListener;

public final class MockHostStateChangeListener implements WatchedNodeListener<HostState> {

  HostState calledWith;

  @Override
  public void onWatchedNodeChange(HostState state) {
    calledWith = state;
    synchronized (this) {
      notifyAll();
    }
  }
}
