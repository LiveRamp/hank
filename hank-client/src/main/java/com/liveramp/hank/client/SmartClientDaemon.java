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

import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.THsHaServer.Args;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.hank.config.SmartClientDaemonConfigurator;
import com.liveramp.hank.config.yaml.YamlSmartClientDaemonConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.generated.SmartClient;
import com.liveramp.hank.util.CommandLineChecker;

/**
 * Run a HankSmartClient inside a Thrift server so non-Java clients can
 * communicate with Hank.
 */
public class SmartClientDaemon {
  private static final Logger LOG = LoggerFactory.getLogger(SmartClientDaemon.class);

  private final SmartClientDaemonConfigurator configurator;
  private final Coordinator coordinator;
  private final String ringGroupName;
  private Thread serverThread;
  private TServer server;
  private Throwable serverReasonFailed;
  private static final int WAITING_FOR_SERVER_MAX_TENTATIVES = 30;
  private static final int WAITING_FOR_SERVER_TIMEOUT_MS = 1000;

  public SmartClientDaemon(SmartClientDaemonConfigurator configurator) {
    this.configurator = configurator;
    this.coordinator = configurator.createCoordinator();
    this.ringGroupName = configurator.getRingGroupName();
  }

  /**
   * start serving the thrift server. doesn't return.
   *
   * @throws IOException
   * @throws TException
   */
  private void serve() throws IOException, TException {
    // set up the service handler
    HankSmartClient handler = new HankSmartClient(coordinator, ringGroupName);

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
    Runnable r = new Runnable() {
      @Override
      public void run() {
        try {
          serve();
        } catch (Throwable t) {
          serverReasonFailed = t;
          LOG.error("Unexpected error in smart client server", t);
        }
      }
    };
    serverThread = new Thread(r, "Smart client server thread");
    serverReasonFailed = null;
    serverThread.start();
    try {
      int tentative = 0;
      // Wait for thrift server to come online
      while (tentative < WAITING_FOR_SERVER_MAX_TENTATIVES &&
          (server == null || !server.isServing()) &&
          serverReasonFailed == null) {
        LOG.debug("Waiting for smart client server to come online...");
        Thread.sleep(WAITING_FOR_SERVER_TIMEOUT_MS);
        ++tentative;
      }
      // Check if Thrift server failed due to an exception
      if (serverReasonFailed != null) {
        throw new RuntimeException("Smart client server failed to start", serverReasonFailed);
      }
      // Check if Thrift server failed to come online
      if (server == null || !server.isServing()) {
        throw new RuntimeException(
            String.format("Waited for smart client server to come online for %d seconds but it did not.",
                (WAITING_FOR_SERVER_MAX_TENTATIVES * WAITING_FOR_SERVER_TIMEOUT_MS) / 1000));
      }
      LOG.debug("Smart client server is online.");
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for smart client server thread to start", e);
    }
  }

  private void run() {
    startServer();
    while (true) {
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
    CommandLineChecker.check(args, new String[]{"configuration_file_path", "log4j_properties_file_path"},
        SmartClientDaemon.class);
    String configPath = args[0];
    String log4jProps = args[1];
    PropertyConfigurator.configure(log4jProps);
    new SmartClientDaemon(new YamlSmartClientDaemonConfigurator(configPath)).run();
  }
}
