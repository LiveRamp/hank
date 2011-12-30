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

package com.rapleaf.hank.client.async;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;

class Connector implements Runnable {

  private static Logger LOG = Logger.getLogger(Connector.class);

  private Thread connectorThread;
  private volatile boolean stopping = false;
  private final LinkedList<HostConnection> connections =
          new LinkedList<HostConnection>();

  @Override
  public void run() {
    while (!stopping) {
      HostConnection connection = null;
      int remaining = 0;
      synchronized (connections) {
        if (connections.size() > 0) {
          connection = connections.pop();
          remaining = connections.size();
        }
      }
      // If there is no connection to connect, do nothing.
      if (connection != null) {
        try {
          connection.attemptConnect();
        } catch (IOException e) {
          LOG.error("Failed to connect", e);
        }
      }
      // If there is no connection waiting, sleep and wait for a new connection.
      if (remaining == 0) {
        try {
          Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
          // We have something to do.
        }
      }
    }
  }

  public void addConnection(HostConnection connection) {
    connection.setConnecting();
    synchronized (connections) {
      connections.addLast(connection);
    }
    connectorThread.interrupt();
  }

  public void stop() {
    stopping = true;
    connectorThread.interrupt();
  }

  public void setConnectorThread(ConnectorThread connectorThread) {
    if (this.connectorThread != null) {
      throw new RuntimeException("Tried to set connector thread but it was already set.");
    }
    this.connectorThread = connectorThread;
  }


}
