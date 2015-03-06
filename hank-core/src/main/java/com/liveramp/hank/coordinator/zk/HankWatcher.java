package com.liveramp.hank.coordinator.zk;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Abstract Watcher implementation that adds a few features.
 * <p/>
 * The watch is immediately reset upon notification, *before* the subclass is
 * notified, guaranteeing that you won't miss notifications. Though, when you do
 * your get() call, you might get get data for events that happened after the
 * original notification, and then be notified *again* for the same data.
 * <p/>
 * The process method is synchronized so that we don't have to worry about
 * concurrent notifications
 * <p/>
 * Calling cancel() on the watcher sets a flag that prevents further
 * notifications and watch-setting.
 */
abstract class HankWatcher implements Watcher {
  private static final Logger LOG = LoggerFactory.getLogger(HankWatcher.class);

  private boolean cancelled = false;

  protected HankWatcher() throws KeeperException, InterruptedException {
    setWatch();
  }

  public abstract void setWatch() throws KeeperException, InterruptedException;

  protected abstract void realProcess(WatchedEvent event);

  @Override
  public final void process(WatchedEvent event) {
    synchronized (this) {
      if (!cancelled) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
          try {
            setWatch();
          } catch (KeeperException e) {
            LOG.error("Failed to reset watch!", e);
          } catch (InterruptedException e) {
            // TODO: support retrying here?
            LOG.error("Failed to reset watch!", e);
          }
          realProcess(event);
        } else {
          // Not sync connected, do nothing
          if (LOG.isDebugEnabled()) {
            LOG.debug("Not sync connected anymore for watched node.");
          }
        }
      }
    }
  }

  public final void cancel() {
    cancelled = true;
  }
}
