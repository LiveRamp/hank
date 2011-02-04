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
package com.rapleaf.hank.part_daemon;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.THsHaServer.Options;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.rapleaf.hank.config.PartDaemonConfigurator;
import com.rapleaf.hank.config.YamlConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DaemonState;
import com.rapleaf.hank.coordinator.DaemonType;
import com.rapleaf.hank.coordinator.Coordinator.DaemonStateChangeListener;
import com.rapleaf.hank.generated.PartDaemon;
import com.rapleaf.hank.util.HostUtils;

public class Server implements DaemonStateChangeListener {
  private static final String HOST_NAME;
  static {
    try {
      HOST_NAME = HostUtils.getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  private final PartDaemonConfigurator configurator;
  private final Coordinator coord;
  private final String ringGroupName;
  private final int ringNumber;
  private Thread serverThread;
  private TServer server;
  private boolean goingDown = false;

  public Server(PartDaemonConfigurator configurator) {
    this.configurator = configurator;
    this.coord = configurator.getCoordinator();
    this.ringGroupName = configurator.getRingGroupName();
    this.ringNumber = configurator.getRingNumber();
    coord.addDaemonStateChangeListener(ringGroupName, ringNumber, HOST_NAME, DaemonType.PART_DAEMON, this);
  }

  public static void main(String[] args) throws IOException, TTransportException {
    String configPath = args[0];
    String log4jprops = args[1];

    PartDaemonConfigurator configurator = new YamlConfigurator(configPath);
    PropertyConfigurator.configure(log4jprops);

    new Server(configurator).run();
  }

  void run() {
    coord.setDaemonState(ringGroupName, ringNumber, HOST_NAME, DaemonType.PART_DAEMON, DaemonState.IDLE);

    while (!goingDown) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // TODO: probably going down.
        break;
      }
    }
  }

  /**
   * start serving the thrift server. doesn't return.
   * @throws TTransportException
   */
  private void serve() throws TTransportException {
    // set up the service handler
    Handler handler = new Handler(configurator);

    // launch the thrift server
    TNonblockingServerSocket serverSocket = new TNonblockingServerSocket(configurator.getServicePort());
    Options options = new Options();
    options.workerThreads = configurator.getNumThreads();
    server = new THsHaServer(new PartDaemon.Processor(handler), serverSocket, options);
    server.serve();
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
    serverThread = new Thread(r, "PartDaemon Thrift Server thread");
    serverThread.start();
    try {
      while (server == null || !server.isServing()) {
        Thread.sleep(100);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for server thread to start", e);
    }
  }

  /**
   * blocks until thrift server is down
   */
  public void stopServer() {
    server.stop();
    try {
      serverThread.join();
    } catch (InterruptedException e) {
      // TODO we're probably shutting down... log a message and continue.
    }
    server = null;
    serverThread = null;
  }

  public void stop() {
    // don't wait to be started again.
    goingDown = true;
    coord.setDaemonState(ringGroupName, ringNumber, HOST_NAME, DaemonType.PART_DAEMON, DaemonState.STOPPING);
    stopServer();
    coord.setDaemonState(ringGroupName, ringNumber, HOST_NAME, DaemonType.PART_DAEMON, DaemonState.IDLE);
  }

  @Override
  public void onDaemonStateChange(String ringGroupName, int ringNumber, String hostName, DaemonType type, DaemonState state) {
    if (state == DaemonState.STARTABLE) {
      coord.setDaemonState(ringGroupName, ringNumber, HOST_NAME, DaemonType.PART_DAEMON, DaemonState.STARTING);
      startServer();
      coord.setDaemonState(ringGroupName, ringNumber, HOST_NAME, DaemonType.PART_DAEMON, DaemonState.STARTED);
    }
    else if (state == DaemonState.STOPPABLE) {
      coord.setDaemonState(ringGroupName, ringNumber, HOST_NAME, DaemonType.PART_DAEMON, DaemonState.STOPPING);
      stopServer();
      coord.setDaemonState(ringGroupName, ringNumber, HOST_NAME, DaemonType.PART_DAEMON, DaemonState.IDLE);
    }
  }
}
