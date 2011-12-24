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
import java.util.concurrent.LinkedBlockingQueue;

class Connector implements Runnable {

  private static Logger LOG = Logger.getLogger(Connector.class);

  private final LinkedBlockingQueue<HostConnection> connections =
      new LinkedBlockingQueue<HostConnection>();

  @Override
  public void run() {
    while (true) {
      HostConnection connection;
      try {
        connection = connections.take();
      } catch (InterruptedException e) {
        LOG.info("Exiting Connecting Thread", e);
        break;
      }
      try {
        connection.attemptConnect();
      } catch (IOException e) {
        LOG.error("Failed to connect", e);
      }
    }
  }

  public void addConnection(HostConnection connection) {
    try {
      connection.setConnecting();
      connections.put(connection);
    } catch (InterruptedException e) {
      LOG.error("Failed to add connection", e);
    }
  }
}
