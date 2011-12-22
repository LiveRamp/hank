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

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

class ConnectingRunnable implements Runnable {

  private static Logger LOG = Logger.getLogger(ConnectingRunnable.class);

  private final LinkedBlockingQueue<AsyncHostConnection> connections =
          new LinkedBlockingQueue<AsyncHostConnection>();
  private final LinkedList<AsyncHostConnection> validConnections;

  ConnectingRunnable(LinkedList<AsyncHostConnection> validConnections) {
    this.validConnections = validConnections;
  }

  @Override
  public void run() {
    while (true) {
      AsyncHostConnection connection;
      try {
        connection = connections.take();
      } catch (InterruptedException e) {
        LOG.info("Exiting Connecting Thread", e);
        break;
      }
      try {
        connection.connect();
        synchronized (validConnections) {
          validConnections.addLast(connection);
        }
      } catch (IOException e) {
        LOG.error("Failed to connect", e);
        addConnection(connection);
      }
    }
  }

  public void addConnection(AsyncHostConnection connection) {
    try {
      connections.put(connection);
    } catch (InterruptedException e) {
      LOG.error("Failed to add connection", e);
    }
  }
}
