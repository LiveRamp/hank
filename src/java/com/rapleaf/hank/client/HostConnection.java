/*
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

package com.rapleaf.hank.client;

import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.PartitionServer;
import com.rapleaf.hank.zookeeper.WatchedNodeListener;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class HostConnection implements WatchedNodeListener<HostState> {

  private static final Logger LOG = Logger.getLogger(HostConnection.class);

  private final int tryLockTimeoutMs;
  private final int establishConnectionTimeoutMs;
  private final int queryTimeoutMs;
  private final int bulkQueryTimeoutMs;
  private TSocket socket;
  private TTransport transport;
  private PartitionServer.Client client;
  private final Host host;
  private final Object stateChangeMutex = new Object();
  protected final ReentrantLock lock = new ReentrantLock(true); // Use a fair ReentrantLock
  private HostConnectionState state = HostConnectionState.DISCONNECTED;

  private static enum HostConnectionState {
    CONNECTED,
    DISCONNECTED,
    // STANDBY: we are waiting for the host to be SERVING
    STANDBY
  }

  // A timeout of 0 means no timeout
  public HostConnection(Host host,
                        int tryLockTimeoutMs,
                        int establishConnectionTimeoutMs,
                        int queryTimeoutMs,
                        int bulkQueryTimeoutMs) throws TException, IOException {
    this.host = host;
    this.tryLockTimeoutMs = tryLockTimeoutMs;
    this.establishConnectionTimeoutMs = establishConnectionTimeoutMs;
    this.queryTimeoutMs = queryTimeoutMs;
    this.bulkQueryTimeoutMs = bulkQueryTimeoutMs;
    host.setStateChangeListener(this);
    onWatchedNodeChange(host.getState());
  }

  Host getHost() {
    return host;
  }

  boolean isAvailable() {
    return state != HostConnectionState.STANDBY;
  }

  private boolean isDisconnected() {
    return state == HostConnectionState.DISCONNECTED;
  }

  private void lock() {
    lock.lock();
  }

  private void unlock() {
    lock.unlock();
  }

  boolean tryLockRespectingFairness() {
    try {
      // Note: tryLock() does not respect fairness, using tryLock(0, unit) instead
      return lock.tryLock(0, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      return false;
    }
  }

  private boolean tryLockWithTimeout() {
    // If configured timeout is 0, wait indefinitely
    if (tryLockTimeoutMs == 0) {
      lock();
      return true;
    }
    // Otherwise, perform a lock with timeout. If interrupted, simply report that we failed to lock.
    try {
      return lock.tryLock(tryLockTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      return false;
    }
  }

  public HankResponse get(int domainId, ByteBuffer key) throws IOException {
    // Lock the connection only if needed
    if (!lock.isHeldByCurrentThread()) {
      // Try to lock within a given timeframe
      if (!tryLockWithTimeout()) {
        throw new IOException("Exceeded timeout while trying to lock the host connection.");
      }
    }
    try {
      // Connect if necessary
      if (isDisconnected()) {
        connect();
      }
      // Check availability
      if (!isAvailable()) {
        throw new IOException("Connection to host is not available (host is not serving).");
      }
      // Query timeout is by default always set to regular mode
      // Perform query
      return client.get(domainId, key);
    } catch (TException e) {
      // Disconnect and give up
      disconnect();
      throw new IOException("Failed to execute get()", e);
    } finally {
      unlock();
    }
  }

  public HankBulkResponse getBulk(int domainId, List<ByteBuffer> keys) throws IOException {
    // Lock the connection only if needed
    if (!lock.isHeldByCurrentThread()) {
      // Try to lock within a given timeframe
      if (!tryLockWithTimeout()) {
        throw new IOException("Exceeded timeout while trying to lock the host connection.");
      }
    }
    try {
      // Connect if necessary
      if (isDisconnected()) {
        connect();
      }
      try {
        // Check availability
        if (!isAvailable()) {
          throw new IOException("Connection to host is not available (host is not serving).");
        }
        // Set socket timeout to bulk mode
        setSocketTimeout(bulkQueryTimeoutMs);
        // Perform query
        return client.getBulk(domainId, keys);
      } finally {
        // Set socket timeout back to regular mode
        setSocketTimeout(queryTimeoutMs);
      }
    } catch (TException e) {
      // Disconnect and give up
      disconnect();
      throw new IOException("Failed to execute getBulk()", e);
    } finally {
      unlock();
    }
  }

  private void disconnect() {
    if (transport != null) {
      transport.close();
    }
    socket = null;
    transport = null;
    client = null;
    state = HostConnectionState.DISCONNECTED;
  }

  private void connect() throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Trying to connect to " + host.getAddress());
    }
    // Use connection timeout to connect
    socket = new TSocket(host.getAddress().getHostName(),
        host.getAddress().getPortNumber(),
        establishConnectionTimeoutMs);
    transport = new TFramedTransport(socket);
    try {
      transport.open();
      // Set socket timeout to regular mode
      setSocketTimeout(queryTimeoutMs);
    } catch (TTransportException e) {
      LOG.error("Failed to establish connection to host.", e);
      disconnect();
      throw new IOException("Failed to establish connection to host.", e);
    }
    TProtocol proto = new TCompactProtocol(transport);
    client = new PartitionServer.Client(proto);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Connection to " + host.getAddress() + " opened.");
    }
    state = HostConnectionState.CONNECTED;
  }

  private void setSocketTimeout(int timeout) {
    if (socket != null) {
      socket.setTimeout(timeout);
    }
  }

  @Override
  public void onWatchedNodeChange(HostState hostState) {
    synchronized (stateChangeMutex) {
      if (hostState != null && hostState == HostState.SERVING) {
        // Reconnect
        lock();
        try {
          disconnect();
          try {
            connect();
          } catch (IOException e) {
            LOG.error("Error connection to host.", e);
          }
        } finally {
          unlock();
        }
      } else {
        // Disconnect and standby
        lock();
        try {
          disconnect();
          state = HostConnectionState.STANDBY;
        } finally {
          unlock();
        }
      }
    }
  }
}
