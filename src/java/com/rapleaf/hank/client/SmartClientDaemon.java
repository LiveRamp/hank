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
package com.rapleaf.hank.client;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.THsHaServer.Args;
import org.apache.thrift.transport.TNonblockingServerSocket;

import com.rapleaf.hank.config.SmartClientDaemonConfigurator;
import com.rapleaf.hank.config.yaml.YamlSmartClientDaemonConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.generated.SmartClient;

/**
 * Run a HankSmartClient inside a Thrift server so non-Java clients can
 * communicate with Hank.
 */
public class SmartClientDaemon {
  private static final Logger LOG = Logger.getLogger(SmartClientDaemon.class);

  private final SmartClientDaemonConfigurator configurator;
  private final Coordinator coord;
  private final String ringGroupName;
  private Thread serverThread;
  private TServer server;

  public SmartClientDaemon(SmartClientDaemonConfigurator configurator) {
    this.configurator = configurator;
    this.coord = configurator.getCoordinator();
    this.ringGroupName = configurator.getRingGroupName();
  }

  /**
   * start serving the thrift server. doesn't return.
   * @throws IOException 
   * @throws TException 
   */
  private void serve() throws IOException, TException {
    // set up the service handler
    HankSmartClient handler = new HankSmartClient(coord, ringGroupName);

    // launch the thrift server
    TNonblockingServerSocket serverSocket = new TNonblockingServerSocket(configurator.getPortNumber());
    Args options = new THsHaServer.Args(serverSocket);
    options.processor(new SmartClient.Processor(handler));
    options.workerThreads(configurator.getNumThreads());
    options.protocolFactory(new TCompactProtocol.Factory());
    server = new THsHaServer(options);
    server.serve();
  }

  public void downServer() {
    server.stop();
    try {
      serverThread.join();
    } catch (InterruptedException e) {
      // we're probably shutting down
      LOG.debug("Interrupted waiting for server thread to exit.", e);
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
        } catch (Exception e) {
          LOG.fatal("Unexpected error in server main loop!", e);
        }
      }
    };
    serverThread = new Thread(r, "Client Thrift Server thread");
    serverThread.start();
    try {
      while (server == null || !server.isServing()) {
        LOG.debug("waiting for smart client daemon server to come online...");
        Thread.sleep(100);
      }
      LOG.debug("smart client server is online.");
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for server thread to start", e);
    }
  }

  private void run() {
    startServer();
    while(true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted main thread. Exiting.", e);
        break;
      }
    }
    downServer();
  }

  public static void main(String[] args) throws Throwable {
    String configPath = args[0];
    String log4jProps = args[1];
    PropertyConfigurator.configure(log4jProps);
    new SmartClientDaemon(new YamlSmartClientDaemonConfigurator(configPath)).run();
  }
}
