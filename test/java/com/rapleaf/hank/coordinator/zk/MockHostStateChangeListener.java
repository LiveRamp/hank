/**
 * 
 */
package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostStateChangeListener;

public final class MockHostStateChangeListener implements HostStateChangeListener {
  Host calledWith;
  @Override
  public void onHostStateChange(Host hostConfig) {
    calledWith = hostConfig;
    synchronized (this) {
      notifyAll();
    }
  }
}