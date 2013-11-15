/**
 *
 */
package com.liveramp.hank.coordinator.zk;

import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostCommandQueueChangeListener;

import static org.junit.Assert.fail;

final class MockHostCommandQueueChangeListener implements HostCommandQueueChangeListener {

  public Host calledWith;

  @Override
  public void onCommandQueueChange(Host hostConfig) {
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
      if (!timeoutOk && end - start > 15000) {
        fail("timed out waiting for notification!");
      }
    }
  }
}
