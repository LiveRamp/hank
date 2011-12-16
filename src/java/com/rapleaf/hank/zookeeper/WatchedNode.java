/**
 *  Copyright 2011 Rapleaf
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.rapleaf.hank.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import java.util.HashSet;
import java.util.Set;

public abstract class WatchedNode<T> {

  private static final Logger LOG = Logger.getLogger(WatchedNode.class);

  private T value;
  private final String nodePath;
  private final ZooKeeperPlus zk;
  private final Set<WatchedNodeListener<T>> listeners = new HashSet<WatchedNodeListener<T>>();
  private boolean cancelled = false;

  private final Watcher watcher = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      // this lock is important so that when changes start happening, we
      // won't run into any concurrency issues

      synchronized (WatchedNode.this) {

        if (!cancelled) {
          if (event.getState() == KeeperState.SyncConnected) {
            // If connected update data and notify listeners
            try {
              if (event.getType().equals(Event.EventType.NodeCreated)) {
                watchForData();
              } else if (event.getType().equals(Event.EventType.NodeDeleted)) {
                watchForCreation();
              } else if (event.getType().equals(Event.EventType.NodeDataChanged)) {
                watchForData();
              }
            } catch (KeeperException e) {
              LOG.error("Exception while trying to update our cached value for " + nodePath, e);
            } catch (InterruptedException e) {
              if (LOG.isTraceEnabled()) {
                LOG.trace("Interrupted while trying to update our cached value for " + nodePath, e);
              }
            }
            synchronized (listeners) {
              for (WatchedNodeListener<T> listener : listeners) {
                listener.onWatchedNodeChange(value);
              }
            }
          } else {
            // Not sync connected, do nothing
            if (LOG.isDebugEnabled()) {
              LOG.debug("Not sync connected anymore for watched node " + nodePath);
            }
          }
        }
      }
    }
  };

  /**
   * Start watching a node, optionnaly waiting for it to be created
   *
   * @param zk
   * @param nodePath
   * @param waitForCreation
   * @throws KeeperException
   * @throws InterruptedException
   */
  protected WatchedNode(final ZooKeeperPlus zk, final String nodePath, boolean waitForCreation)
      throws KeeperException, InterruptedException {
    this.zk = zk;
    this.nodePath = nodePath;
    // Immediately try to load the data, if it fails, then optionally wait
    try {
      watchForData();
    } catch (KeeperException.NoNodeException e) {
      if (waitForCreation) {
        NodeCreationBarrier.block(zk, nodePath);
        watchForData();
      } else {
        watchForCreation();
      }
    }
  }

  public void addListener(WatchedNodeListener<T> listener) {
    synchronized (listeners) {
      listeners.add(listener);
    }
  }

  public boolean removeListener(WatchedNodeListener<T> listener) {
    synchronized (listeners) {
      return listeners.remove(listener);
    }
  }

  private void watchForCreation() throws InterruptedException, KeeperException {
    value = null;
    zk.exists(nodePath, watcher);
  }

  private void watchForData() throws InterruptedException, KeeperException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format("Getting value for %s", nodePath));
    }
    value = decode(zk.getData(nodePath, watcher, new Stat()));
  }

  protected abstract T decode(byte[] data);

  public T get() {
    return value;
  }

  public void set(T v) throws KeeperException, InterruptedException {
    zk.setData(nodePath, encode(v), -1);
  }

  public void cancelWatch() {
    cancelled = true;
  }

  protected abstract byte[] encode(T v);

}
