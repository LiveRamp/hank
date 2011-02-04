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
package com.rapleaf.tiamat.client;

import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.THsHaServer.Options;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.rapleaf.tiamat.config.PartDaemonConfigurator;
import com.rapleaf.tiamat.coordinator.Coordinator;
import com.rapleaf.tiamat.exception.DataNotFoundException;
import com.rapleaf.tiamat.generated.Tiamat;

public class Server {

  private final PartDaemonConfigurator configurator;
  private final Coordinator coord;
  private final String ringGroupName;
  private Thread serverThread;
  private TServer server;
  
  public Server(PartDaemonConfigurator configurator, String ringGroupName) {
    this.configurator = configurator;
    this.coord = configurator.getCoordinator();
    this.ringGroupName = ringGroupName;
  }

  /**
   * start serving the thrift server. doesn't return.
   * @throws TTransportException
   */
  private void serve() throws TTransportException {
    // set up the service handler
    Handler handler;
    try {
      handler = new Handler(coord, ringGroupName);
    } catch (DataNotFoundException e) {
      throw new RuntimeException(e);
    }

    // launch the thrift server
    TNonblockingServerSocket serverSocket = new TNonblockingServerSocket(configurator.getServicePort());
    Options options = new Options();
    options.workerThreads = configurator.getNumThreads();
    server = new THsHaServer(new Tiamat.Processor(handler), serverSocket, options);
    server.serve();
  }

  public void downServer() {
  server.stop();
  try {
    serverThread.join();
  } catch (InterruptedException e) {
    // TODO we're probably shutting down... log a message and continue.
  }
  server = null;
  serverThread = null;
  }

  /**
   * Start serving the Thrift server. Returns when the server is up.
   */
  public void startServer() {
    Runnable r = new Runnable(){
      @Override
      public void run() {
        try {
          serve();
        } catch (TTransportException e) {
          // TODO deal with exception. server is probably going down unexpectedly
        }
      }
    };
    serverThread = new Thread(r, "Client Thrift Server thread");
    serverThread.start();
    try {
      while (server == null || !server.isServing()) {
        Thread.sleep(100);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for server thread to start", e);
    }
  }
}
