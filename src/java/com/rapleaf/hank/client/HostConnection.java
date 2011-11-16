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
import com.rapleaf.hank.coordinator.HostStateChangeListener;
import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.PartitionServer;
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
import java.util.concurrent.locks.ReentrantLock;

public class HostConnection implements HostStateChangeListener {

  private static final Logger LOG = Logger.getLogger(HostConnection.class);

  private final int timeoutMS;
  public TTransport transport;
  public PartitionServer.Client client;
  private final Host host;
  private final Object stateChangeMutex = new Object();
  private final ReentrantLock lock = new ReentrantLock();
  private HostConnectionState state = HostConnectionState.DISCONNECTED;

  private static enum HostConnectionState {
    CONNECTED,
    DISCONNECTED,
    // STANDBY: we are waiting for the host to be SERVING
    STANDBY
  }

  // A timeout of 0 means no timeout
  public HostConnection(Host host, int timeoutMS) throws TException, IOException {
    this.host = host;
    this.timeoutMS = timeoutMS;
    host.setStateChangeListener(this);
    onHostStateChange(host);
  }

  public Host getHost() {
    return host;
  }

  public boolean isAvailable() {
    return state != HostConnectionState.STANDBY;
  }

  public boolean isDisconnected() {
    return state == HostConnectionState.DISCONNECTED;
  }

  void lock() {
    lock.lock();
  }

  void unlock() {
    lock.unlock();
  }

  boolean tryLock() {
    return lock.tryLock();
  }

  public HankResponse get(int domainId, ByteBuffer key) throws IOException {
    // Lock the connection only if needed
    if (!lock.isHeldByCurrentThread()) {
      lock();
    }
    if (!isAvailable()) {
      throw new IOException("Connection to host is not available (host is not serving).");
    }
    try {
      // Connect if necessary
      if (isDisconnected()) {
        connect();
      }
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
      lock();
    }
    if (!isAvailable()) {
      throw new IOException("Connection to host is not available (host is not serving).");
    }
    try {
      // Connect if necessary
      if (isDisconnected()) {
        connect();
      }
      // Perform query
      return client.getBulk(domainId, keys);
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
    transport = null;
    client = null;
    state = HostConnectionState.DISCONNECTED;
  }

  private void connect() throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Trying to connect to " + host.getAddress());
    }
    transport = new TFramedTransport(new TSocket(host.getAddress().getHostName(),
        host.getAddress().getPortNumber(),
        timeoutMS));
    try {
      transport.open();
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

  @Override
  public void onHostStateChange(Host host) {
    synchronized (stateChangeMutex) {
      try {
        switch (host.getState()) {
          case SERVING:
            lock();
            try {
              disconnect();
              connect();
            } finally {
              unlock();
            }
            break;

          default:
            lock();
            try {
              disconnect();
              state = HostConnectionState.STANDBY;
            } finally {
              unlock();
            }
        }
      } catch (IOException e) {
        LOG.error("Exception while trying to get host state!", e);
      }
    }
  }
}
