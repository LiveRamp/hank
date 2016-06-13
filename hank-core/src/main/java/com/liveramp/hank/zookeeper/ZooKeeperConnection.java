/**
 *  Copyright 2011 LiveRamp
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
package com.liveramp.hank.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * Base class that should be used by any class intending to connect to the
 * ZooKeeper service. This class automatically handles connecting,
 * disconnecting, and session expiry, and provides a clean interface for any
 * subclasses to take action upon these three notable events.
 */
public class ZooKeeperConnection implements Watcher {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperConnection.class);

  public static final int DEFAULT_SESSION_TIMEOUT = 30000;
  public static final int DEFAULT_MAX_ATTEMPTS = 5;
  public static final int CONNECT_DELAY = 100; // ms
  public static final int MAX_CONNECT_DELAY = 7500; // ms

  protected ZooKeeperPlus zk;

  /**
   * Used to block while disconnected. Use {@link #waitForConnection()} in
   * subclasses to block while disconnected.
   */
  private CountDownLatch connectedSignal = new CountDownLatch(1);

  private String connectString;
  private int maxConnectAttempts;

  /**
   * Creates a new connection to the ZooKeeper service. Blocks until we are
   * connected to the service. Uses the default session timeout of 30 seconds.
   *
   * @param connectString comma separated host:port pairs, each corresponding to a zk
   *                      server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
   * @throws InterruptedException
   */
  public ZooKeeperConnection(String connectString) throws InterruptedException {
    this(connectString, DEFAULT_SESSION_TIMEOUT);
  }

  /**
   * Creates a new connection to the ZooKeeper service. Blocks until we are
   * connected to the service.
   *
   * @param connectString  comma separated host:port pairs, each corresponding to a zk
   *                       server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
   * @param sessionTimeout session timeout in milliseconds
   * @throws InterruptedException
   */
  public ZooKeeperConnection(String connectString, int sessionTimeout) throws InterruptedException {
    this(connectString, sessionTimeout, DEFAULT_MAX_ATTEMPTS);
  }

  /**
   * Creates a new connection to the ZooKeeper service. Blocks until we are
   * connected to the service.
   *
   * @param connectString      comma separated host:port pairs, each corresponding to a zk
   *                           server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
   * @param sessionTimeout     session timeout in milliseconds
   * @param maxConnectAttempts how many times we should try to connect to the ZooKeeper ensemble
   *                           before dying
   * @throws InterruptedException
   */
  public ZooKeeperConnection(String connectString, int sessionTimeout, int maxConnectAttempts) throws InterruptedException {
    this.connectString = connectString;
    this.maxConnectAttempts = maxConnectAttempts;

    LOG.info("ZooKeeperConnection.connectString = "+connectString);
    LOG.info("ZooKeeperConnection.sessionTimeout = "+sessionTimeout);
    LOG.info("ZooKeeperConnection.maxConnectionAttempts = "+maxConnectAttempts);

    this.zk = new ZooKeeperPlus(connectString, sessionTimeout, this);

    try {
      //  TODO not sure what the right way to do this is.  by using a finite limit here, we avoid hanging for eternity on startup,
      //  but then on KeeperState.Expired, don't give up.
      connect(DEFAULT_MAX_ATTEMPTS);
    } catch (IOException e) {
      // If we can't connect, then die so that someone can reconfigure.
      LOG.error("Failed to connect to the ZooKeeper service", e);
      throw new RuntimeException(e);
    }
    connectedSignal.await();
  }

  /**
   * Discards the current connection (if there is one), and tries to set up a
   * new connection to the ZooKeeper service.
   *
   * @param maxConnectAttempts the maximum number of times we want to connect to the ZooKeeper
   *                           ensemble. One attempt means trying all the servers once. A value
   *                           of zero means to attempt to connect forever.
   * @throws IOException if all of our connection attempts failed
   */
  private void connect(int maxConnectAttempts) throws IOException {
    int attempts = 0;
    int delay = CONNECT_DELAY;
    while (true) {
      try {
        LOG.info("Attempting ZooKeeperReconnect");
        zk.reconnect();
        // We return as soon as the assignment has succeeded.
        return;
      } catch (IOException e) {
        //this means that we tried to connect to all the hosts, but they all failed
        attempts++;
        // if maxConnectAttempts == 0, then try forever
        if (maxConnectAttempts != 0 && attempts >= maxConnectAttempts) {
          throw e;
        }
        delay *= 2; //use exponential backoff
        delay = Math.min(delay, MAX_CONNECT_DELAY);
      }
      try {
        LOG.info("Reconnect failed, sleeping for "+delay+" ms");
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        // Someone wants us to stop connecting
        return;
      }
    }
  }

  /**
   * Listens for notifications from the ZooKeeper service telling that we have
   * been connected, disconnected, or our session has expired.
   *
   * Upon connection, we first make a call to {@link #onConnect()}, and then we
   * release all threads that are blocking on {@link #waitForConnection()}.
   *
   * Upon disconnection, we call {@link #onDisconnect()}, and then we reset the
   * latch to block any threads that call {@link #waitForConnection()}.
   *
   * On session expiry, we call {@link #onSessionExpire()}, reset the latch, and
   * then manually try to reconnect to the ZooKeeper service.
   *
   * @param event
   */
  @Override
  public void process(WatchedEvent event) {
    if (event.getType() == Event.EventType.None) {
      KeeperState state = event.getState();
      LOG.info("Getting event: "+state);
      switch (state) {
        case SyncConnected:
          onConnect();
          connectedSignal.countDown();
          break;
        case Disconnected:
          onDisconnect();
          connectedSignal = new CountDownLatch(1);
          break;
        case Expired:
          onSessionExpire();
          connectedSignal = new CountDownLatch(1);
          try {
            LOG.info("Attempting ZooKeeper reconnect");
            connect(maxConnectAttempts);
          } catch (IOException e) {
            LOG.error("Failed to connect to the ZooKeeper service", e);
            throw new RuntimeException("Couldn't connect to the ZooKeeper service", e);
          }
          break;
      }
      // Return because we are done processing this event; do not let subclasses
      // process.
      return;
    }
  }

  /**
   * Allows for subclasses to block until we are connected to the ZooKeeper
   * service. Returns immediately if we are already connected.
   *
   * @throws InterruptedException
   */
  protected void waitForConnection() throws InterruptedException {
    connectedSignal.await();
  }

  /**
   * Called when a connection to the ZooKeeper service has been established.
   * Meant to be used by subclasses
   */
  protected void onConnect() {
  }

  /**
   * Called when the connection to the ZooKeeper service has been broken. Note
   * that a disconnect does not mean our session has expired. If the connection
   * can be reestablished before the session timeout, we will keep the same
   * session (which means that ephemeral nodes will stay alive).
   */
  protected void onDisconnect() {
  }

  /**
   * Called when our session with the ZooKeeper service has expired.
   */
  protected void onSessionExpire() {
  }

  public String getConnectString() {
    return connectString;
  }
}
