/**
 * 
 */
package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.coordinator.HostCommandQueueChangeListener;
import com.rapleaf.hank.coordinator.HostConfig;

final class MockHostCommandQueueChangeListener implements HostCommandQueueChangeListener {
  public HostConfig calledWith;
  @Override
  public void onCommandQueueChange(HostConfig hostConfig) {
    calledWith = hostConfig;
    synchronized (this) {
      notifyAll();
    }
  }

  public void waitForNotification() throws Exception {
    waitForNotification(false);
  }

  public void waitForNotification(boolean timeoutOk) throws Exception {
    synchronized (this) {
      if (calledWith != null) {
        return;
      }
      long start = System.currentTimeMillis();
      this.wait(15000);
      long end = System.currentTimeMillis();
      if (calledWith != null) {
        return;
      }
      if (!timeoutOk && end-start > 15000) {
        TestZkHostConfig.fail("timed out waiting for notification!");
      }
    }
  }
}