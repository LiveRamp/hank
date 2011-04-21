/**
 * 
 */
package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostStateChangeListener;

public final class MockHostStateChangeListener implements HostStateChangeListener {
  HostConfig calledWith;
  @Override
  public void onHostStateChange(HostConfig hostConfig) {
    calledWith = hostConfig;
    synchronized (this) {
      notifyAll();
    }
  }
}