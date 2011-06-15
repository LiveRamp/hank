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

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostStateChangeListener;
import com.rapleaf.hank.generated.PartDaemon;
import com.rapleaf.hank.generated.PartDaemon.Client;

final class PartDaemonConnection implements HostStateChangeListener {
  private static final Logger LOG = Logger.getLogger(PartDaemonConnection.class);

  private final Lock lock = new ReentrantLock();

  public TTransport transport;
  public Client client;

  private final Host hostConfig;

  private final Object stateChangeMutex = new Object();

  private boolean closed = true;

  public PartDaemonConnection(Host hostConfig) throws TException, IOException {
    this.hostConfig = hostConfig;
    hostConfig.setStateChangeListener(this);
    onHostStateChange(hostConfig);
  }

  public void lock() {
    lock.lock();
  }

  public void unlock() {
    lock.unlock();
  }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public void onHostStateChange(Host hostConfig) {
    synchronized (stateChangeMutex) {
      try {
        switch (hostConfig.getState()) {
          case SERVING:
            connect();
            break;

          default:
            disconnect();
        }
      } catch (IOException e) {
        LOG.error("Exception while trying to get host state!", e);
      }
    }
  }

  private void disconnect() {
    if (!isClosed()) {
      lock.lock();
      closed = true;
      transport.close();
      transport = null;
      client = null;
      lock.unlock();
    }
  }

  private void connect() {
    if (isClosed()) {
      LOG.trace("Trying to connect to " + hostConfig.getAddress() + ", waiting on the lock...");
      lock.lock();
      transport = new TFramedTransport(new TSocket(hostConfig.getAddress().getHostName(), hostConfig.getAddress().getPortNumber()));
      try {
        transport.open();
      } catch (TTransportException e) {
        LOG.error("Failed to establish connection to host!", e);
        transport = null;
        return;
      }
      TProtocol proto = new TCompactProtocol(transport);
      client = new PartDaemon.Client(proto);
      closed = false;
      LOG.trace("Connection to " + hostConfig.getAddress() + " opened!");
      lock.unlock();
    }
  }
}