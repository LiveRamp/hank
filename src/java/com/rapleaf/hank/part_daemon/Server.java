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

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.THsHaServer.Args;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.rapleaf.hank.config.PartDaemonConfigurator;
import com.rapleaf.hank.config.YamlConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.PartDaemonState;
import com.rapleaf.hank.coordinator.HostConfig.HostStateChangeListener;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.generated.PartDaemon;
import com.rapleaf.hank.generated.PartDaemon.Iface;
import com.rapleaf.hank.util.HostUtils;

public class Server implements HostStateChangeListener {
  private static final Logger LOG = Logger.getLogger(Server.class);

  private final PartDaemonConfigurator configurator;
  private final Coordinator coord;
  private Thread serverThread;
  private TServer server;
  private boolean goingDown = false;
  private final PartDaemonAddress hostAddress;
  private final Object mutex = new Object();

  private final HostConfig hostConfig;

  public Server(PartDaemonConfigurator configurator, String hostName) throws IOException, DataNotFoundException {
    this.configurator = configurator;
    this.coord = configurator.getCoordinator();
    hostAddress = new PartDaemonAddress(hostName, configurator.getServicePort());
    hostConfig = coord.getRingGroupConfig(configurator.getRingGroupName()).getRingConfigForHost(hostAddress).getHostConfigByAddress(hostAddress);
    hostConfig.setStateChangeListener(this);
  }

  public static void main(String[] args) throws IOException, TTransportException, DataNotFoundException {
    String configPath = args[0];
    String log4jprops = args[1];

    PartDaemonConfigurator configurator = new YamlConfigurator(configPath);
    PropertyConfigurator.configure(log4jprops);

    new Server(configurator, HostUtils.getHostName()).run();
  }

  public void run() throws IOException {
    hostConfig.partDaemonOnline();
//    coord.setDaemonState(ringGroupName, ringNumber, hostAddress, DaemonType.PART_DAEMON, DaemonState.IDLE);

    while (!goingDown) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // TODO: probably going down.
        break;
      }
    }
    hostConfig.partDaemonOffline();
  }

  /**
   * start serving the thrift server. doesn't return.
   * @throws TTransportException
   * @throws IOException 
   * @throws DataNotFoundException 
   */
  private void serve() throws TTransportException, DataNotFoundException, IOException {
    // set up the service handler
    Iface handler = getHandler();

    // launch the thrift server
    TNonblockingServerSocket serverSocket = new TNonblockingServerSocket(configurator.getServicePort());
    Args options = new Args(serverSocket);
    options.processor(new PartDaemon.Processor(handler));
    options.workerThreads(configurator.getNumThreads());
    server = new THsHaServer(options);
    LOG.debug("Launching Thrift server...");
    server.serve();
    LOG.debug("Thrift server exited.");
  }

  protected Iface getHandler() throws DataNotFoundException, IOException {
    return new Handler(hostAddress, configurator);
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

  public void stop() throws IOException {
    // don't wait to be started again.
    goingDown = true;
    setState(PartDaemonState.STOPPING);
    stopServer();
    setState(PartDaemonState.IDLE);
  }

  private void setState(PartDaemonState state) throws IOException {
    hostConfig.setPartDaemonState(state);
  }

  @Override
  public void onHostStateChange(HostConfig hostConfig) {
    synchronized (mutex) {
      PartDaemonState state;
      try {
        state = hostConfig.getPartDaemonState();
        LOG.debug("Notified of state change to state " + state);
        switch (state) {
          case STARTABLE:
            setState(PartDaemonState.STARTING);
            startServer();
            setState(PartDaemonState.STARTED);
            break;

          case STOPPABLE:
            setState(PartDaemonState.STOPPING);
            stopServer();
            setState(PartDaemonState.IDLE);
            break;

          default:
            LOG.debug("notified of an irrelevant state: " + state);
        }
      } catch (IOException e) {
        LOG.error("Error processing host state change!", e);
        return;
      }
      
    }
  }
}
