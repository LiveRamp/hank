/*
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

package com.liveramp.hank.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.generated.HankBulkResponse;
import com.liveramp.hank.generated.HankResponse;
import com.liveramp.hank.generated.PartitionServer;
import com.liveramp.hank.zookeeper.WatchedNodeListener;

public class HostConnection implements WatchedNodeListener<HostState> {

  private static final Logger LOG = LoggerFactory.getLogger(HostConnection.class);

  private final int tryLockTimeoutMs;
  private final int establishConnectionTimeoutMs;
  private final int queryTimeoutMs;
  private final int bulkQueryTimeoutMs;
  private TSocket socket;
  private TTransport transport;
  private PartitionServer.Client client;
  private final Host host;
  protected final ReentrantLock lock = new ReentrantLock(true); // Use a fair ReentrantLock

  // A timeout of 0 means no timeout
  public HostConnection(Host host,
                        int tryLockTimeoutMs,
                        int establishConnectionTimeoutMs,
                        int queryTimeoutMs,
                        int bulkQueryTimeoutMs) throws IOException {
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

  boolean isServing() {
    try {
      return HostState.SERVING.equals(host.getState());
    } catch (IOException e) {
      return false;
    }
  }

  boolean isOffline() {
    try {
      return HostState.OFFLINE.equals(host.getState());
    } catch (IOException e) {
      return false;
    }
  }

  private boolean isDisconnected() {
    return client == null;
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
      // Check availability
      if (!isServing() && !isOffline()) {
        throw new IOException("Connection to host is not available (host is not serving).");
      }
      // Connect if necessary
      if (isDisconnected()) {
        connect();
      }
      // Query timeout is by default always set to regular mode
      // Perform query
      HankResponse result = client.get(domainId, key);
      if (result.is_set_xception()) {
        throw new IOException("Server failed to execute GET: " + result.get_xception());
      } else {
        return result;
      }
    } catch (TException e) {
      // Disconnect and give up
      disconnect();
      throw new IOException("Failed to execute GET", e);
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
      // Check availability
      if (!isServing() && !isOffline()) {
        throw new IOException("Connection to host is not available (host is not serving).");
      }
      // Connect if necessary
      if (isDisconnected()) {
        connect();
      }
      try {
        // Set socket timeout to bulk mode
        setSocketTimeout(bulkQueryTimeoutMs);
        // Perform query
        HankBulkResponse result = client.getBulk(domainId, keys);
        if (result.is_set_xception()) {
          throw new IOException("Server failed to execute GET BULK: " + result.get_xception());
        } else {
          return result;
        }
      } finally {
        // Set socket timeout back to regular mode
        setSocketTimeout(queryTimeoutMs);
      }
    } catch (TException e) {
      // Disconnect and give up
      disconnect();
      throw new IOException("Failed to execute GET BULK", e);
    } finally {
      unlock();
    }
  }

  public void disconnect() {
    if (transport != null) {
      transport.close();
    }
    socket = null;
    transport = null;
    client = null;
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
      LOG.error("Failed to establish connection to host " + host.getAddress(), e);
      disconnect();
      throw new IOException("Failed to establish connection to host " + host.getAddress(), e);
    }
    TProtocol proto = new TCompactProtocol(transport);
    client = new PartitionServer.Client(proto);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Connection to " + host.getAddress() + " opened.");
    }
  }

  private void setSocketTimeout(int timeout) {
    if (socket != null) {
      socket.setTimeout(timeout);
    }
  }

  @Override
  public void onWatchedNodeChange(HostState hostState) {
    if (hostState != null && hostState == HostState.SERVING) {
      // Reconnect
      lock();
      try {
        disconnect();
        try {
          connect();
        } catch (IOException e) {
          LOG.error("Error connecting to host " + host.getAddress(), e);
        }
      } finally {
        unlock();
      }
    } else {
      // Disconnect and standby
      lock();
      try {
        disconnect();
      } finally {
        unlock();
      }
    }
  }

  public boolean isLocked() {
    return lock.isLocked();
  }

  @Override
  public String toString() {
    return "HostConnection{" +
        "tryLockTimeoutMs=" + tryLockTimeoutMs +
        ", establishConnectionTimeoutMs=" + establishConnectionTimeoutMs +
        ", queryTimeoutMs=" + queryTimeoutMs +
        ", bulkQueryTimeoutMs=" + bulkQueryTimeoutMs +
        ", socket=" + socket +
        ", transport=" + transport +
        ", client=" + client +
        ", host=" + host +
        ", lock=" + lock +
        '}';
  }
}
