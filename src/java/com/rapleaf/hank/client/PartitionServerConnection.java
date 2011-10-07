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
import com.rapleaf.hank.generated.PartitionServer.Client;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

final class PartitionServerConnection implements HostStateChangeListener {
  private static final Logger LOG = Logger.getLogger(PartitionServerConnection.class);

  private final Lock lock = new ReentrantLock();

  public TTransport transport;
  public Client client;

  private final Host host;

  private final Object stateChangeMutex = new Object();

  private PartitionServerConnectionState state = PartitionServerConnectionState.DISCONNECTED;

  private static enum PartitionServerConnectionState {
    CONNECTED,
    DISCONNECTED,
    // STANDBY: we are waiting for the host to be SERVING
    STANDBY
  }

  public PartitionServerConnection(Host host) throws TException, IOException {
    this.host = host;
    host.setStateChangeListener(this);
    onHostStateChange(host);
  }

  public Host getHost() {
    return host;
  }

  @Override
  public void onHostStateChange(Host host) {
    synchronized (stateChangeMutex) {
      try {
        switch (host.getState()) {
          case SERVING:
            lock();
            disconnect();
            connect();
            unlock();
            break;

          default:
            lock();
            disconnect();
            state = PartitionServerConnectionState.STANDBY;
            unlock();
        }
      } catch (IOException e) {
        LOG.error("Exception while trying to get host state!", e);
      }
    }
  }

  public boolean isAvailable() {
    return state != PartitionServerConnectionState.STANDBY;
  }

  public boolean isDisconnected() {
    return state == PartitionServerConnectionState.DISCONNECTED;
  }

  public HankResponse get(int domainId, ByteBuffer key) throws IOException {
    if (!isAvailable()) {
      throw new IOException("Connection is not available.");
    }
    lock();
    // Connect if necessary
    if (isDisconnected()) {
      connect();
    }
    try {
      return client.get(domainId, key);
    } catch (TException e1) {
      // Reconnect and retry
      LOG.trace("Failed to execute get(), reconnecting and retrying...");
      disconnect();
      connect();
      try {
        return client.get(domainId, key);
      } catch (TException e2) {
        throw new IOException("Failed to execute get() again, giving up.", e2);
      }
    } finally {
      unlock();
    }
  }

  public HankBulkResponse getBulk(int domainId, List<ByteBuffer> keys) throws IOException {
    if (!isAvailable()) {
      throw new IOException("Connection is not available.");
    }
    lock();
    // Connect if necessary
    if (isDisconnected()) {
      connect();
    }
    try {
      return client.getBulk(domainId, keys);
    } catch (TException e1) {
      // Reconnect and retry
      LOG.trace("Failed to execute getBulk(), reconnecting and retrying...");
      disconnect();
      connect();
      try {
        return client.getBulk(domainId, keys);
      } catch (TException e2) {
        throw new IOException("Failed to execute getBulk() again, giving up.", e2);
      }
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
    state = PartitionServerConnectionState.DISCONNECTED;
  }

  private void connect() throws IOException {
    LOG.trace("Trying to connect to " + host.getAddress());
    transport = new TFramedTransport(new TSocket(host.getAddress().getHostName(), host.getAddress().getPortNumber()));
    try {
      transport.open();
    } catch (TTransportException e) {
      LOG.error("Failed to establish connection to host.", e);
      disconnect();
      throw new IOException("Failed to establish connection to host.", e);
    }
    TProtocol proto = new TCompactProtocol(transport);
    client = new PartitionServer.Client(proto);
    LOG.trace("Connection to " + host.getAddress() + " opened.");
    state = PartitionServerConnectionState.CONNECTED;
  }

  private void lock() {
    lock.lock();
  }

  private void unlock() {
    lock.unlock();
  }
}
