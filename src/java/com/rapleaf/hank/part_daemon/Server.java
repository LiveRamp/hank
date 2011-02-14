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

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.THsHaServer.Options;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.rapleaf.hank.config.PartDaemonAddress;
import com.rapleaf.hank.config.PartDaemonConfigurator;
import com.rapleaf.hank.config.YamlConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DaemonState;
import com.rapleaf.hank.coordinator.DaemonType;
import com.rapleaf.hank.coordinator.Coordinator.DaemonStateChangeListener;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.generated.PartDaemon;
import com.rapleaf.hank.util.HostUtils;

public class Server implements DaemonStateChangeListener {
  private static final Logger LOG = Logger.getLogger(Server.class);
  
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
  private final PartDaemonAddress hostAddress;
  private final Object mutex = new Object();

  public Server(PartDaemonConfigurator configurator) {
    this.configurator = configurator;
    this.coord = configurator.getCoordinator();
    this.ringGroupName = configurator.getRingGroupName();
    this.ringNumber = configurator.getRingNumber();
    hostAddress = new PartDaemonAddress(HOST_NAME, configurator.getServicePort());
    coord.addDaemonStateChangeListener(ringGroupName, ringNumber, hostAddress, DaemonType.PART_DAEMON, this);
  }

  public static void main(String[] args) throws IOException, TTransportException {
    String configPath = args[0];
    String log4jprops = args[1];

    PartDaemonConfigurator configurator = new YamlConfigurator(configPath);
    PropertyConfigurator.configure(log4jprops);

    new Server(configurator).run();
  }

  void run() {
//    coord.setDaemonState(ringGroupName, ringNumber, hostAddress, DaemonType.PART_DAEMON, DaemonState.IDLE);

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
   * @throws IOException 
   * @throws DataNotFoundException 
   */
  private void serve() throws TTransportException, DataNotFoundException, IOException {
    // set up the service handler
    Handler handler = new Handler(hostAddress, configurator);

    // launch the thrift server
    TNonblockingServerSocket serverSocket = new TNonblockingServerSocket(configurator.getServicePort());
    Options options = new Options();
    options.workerThreads = configurator.getNumThreads();
    server = new THsHaServer(new PartDaemon.Processor(handler), serverSocket, options);
    LOG.debug("Launching Thrift server...");
    server.serve();
    LOG.debug("Thrift server exited.");
  }

  /**
   * Start serving the Thrift server. Returns when the server is up.
   */
  public void startServer() {
    if (server == null) {
      Runnable r = new Runnable(){
        @Override
        public void run() {
          try {
            serve();
          } catch (Exception e) {
            // TODO deal with exception. server is probably going down unexpectedly
            LOG.fatal("Server thread died with exception!", e);
          }
        }
      };
      serverThread = new Thread(r, "PartDaemon Thrift Server thread");
      LOG.info("Launching server thread...");
      serverThread.start();
      try {
        while (server == null || !server.isServing()) {
          LOG.debug("Server isn't online yet. Waiting...");
          Thread.sleep(1000);
        }
        LOG.info("Thrift server online and serving.");
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted waiting for server thread to start", e);
      }
    } else {
      LOG.info("Told to start, but server was already running.");
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
    setState(DaemonState.STOPPING);
    stopServer();
    setState(DaemonState.IDLE);
  }

  @Override
  public void onDaemonStateChange(String ringGroupName, int ringNumber, PartDaemonAddress hostAddress, DaemonType type, DaemonState state) {
    synchronized (mutex) {
      LOG.debug("Notified of state change to state " + state);
      switch (state) {
        case STARTABLE:
          setState(DaemonState.STARTING);
          startServer();
          setState(DaemonState.STARTED);
          break;

        case STOPPABLE:
          setState(DaemonState.STOPPING);
          stopServer();
          setState(DaemonState.IDLE);
          break;

        default:
          LOG.debug("notified of an irrelevant state: " + state);
      }
    }
  }

  private void setState(DaemonState state) {
    coord.setDaemonState(ringGroupName, ringNumber, hostAddress, DaemonType.PART_DAEMON, state);
  }
}
