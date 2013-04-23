/**
 *
 */
package com.liveramp.hank.coordinator.zk;

import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.zookeeper.WatchedNodeListener;

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
