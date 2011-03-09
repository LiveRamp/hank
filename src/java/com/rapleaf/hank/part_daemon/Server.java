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

import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.config.PartservConfigurator;
import com.rapleaf.hank.config.YamlPartservConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.coordinator.HostConfig.HostStateChangeListener;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.generated.PartDaemon;
import com.rapleaf.hank.generated.PartDaemon.Iface;
import com.rapleaf.hank.util.HostUtils;

public class Server implements HostStateChangeListener {
  private static final Logger LOG = Logger.getLogger(Server.class);

  private final PartservConfigurator configurator;
  private final Coordinator coord;
  private Thread serverThread;
  private TServer server;
  private boolean goingDown = false;
  private final PartDaemonAddress hostAddress;
  private final Object mutex = new Object();

  private final HostConfig hostConfig;

  private Thread updateThread;

  private final RingGroupConfig ringGroupConfig;

  private final RingConfig ringConfig;

  public Server(PartservConfigurator configurator, String hostName) throws IOException, DataNotFoundException {
    this.configurator = configurator;
    this.coord = configurator.getCoordinator();
    hostAddress = new PartDaemonAddress(hostName, configurator.getServicePort());
    ringGroupConfig = coord.getRingGroupConfig(configurator.getRingGroupName());
    ringConfig = ringGroupConfig.getRingConfigForHost(hostAddress);
    hostConfig = ringConfig.getHostConfigByAddress(hostAddress);
    hostConfig.setStateChangeListener(this);
  }

  public static void main(String[] args) throws IOException, TTransportException, DataNotFoundException, InvalidConfigurationException {
    String configPath = args[0];
    String log4jprops = args[1];

    PartservConfigurator configurator = new YamlPartservConfigurator(configPath);
    PropertyConfigurator.configure(log4jprops);

    new Server(configurator, HostUtils.getHostName()).run();
  }

  public void run() throws IOException {
    hostConfig.setState(HostState.IDLE);

    while (!goingDown) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // TODO: probably going down.
        break;
      }
    }
    hostConfig.setState(HostState.OFFLINE);
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
  private void startServer() {
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
  private void stopServer() {
    if (server == null) {
      return;
    }

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
    stopServer();
    setState(HostState.IDLE);
  }

  private void setState(HostState state) throws IOException {
    hostConfig.setState(state);
  }

  @Override
  public void onHostStateChange(HostConfig hostConfig) {
    synchronized (mutex) {
      HostCommand command;
      try {
        command = hostConfig.getCommand();
        HostState state = hostConfig.getState();
        LOG.debug("Notified of host state change. Current command: " + command);
        switch (command) {
          case SERVE_DATA:
            switch (state) {
              case IDLE:
                startServer();
                setState(HostState.SERVING);
                break;
              default:
                LOG.debug("have command " + command
                    + " but not compatible with current state " + state
                    + ". Ignoring.");
            }
            break;

          case GO_TO_IDLE:
            switch (state) {
              case SERVING:
                stopServer();
                setState(HostState.IDLE);
                break;
              case UPDATING:
                LOG.debug("received command " + command
                    + " but current state is " + state
                    + ", which cannot be stopped. Will wait until completion.");
                break;
              default:
                LOG.debug("have command " + command
                    + " but not compatible with current state " + state
                    + ". Ignoring.");
            }
            break;

          case EXECUTE_UPDATE:
            switch (state) {
              case IDLE:
                setState(HostState.UPDATING);
                update();
                break;
              case SERVING:
                LOG.debug("Going directly from SERVING to UPDATING is not currently supported.");
              default:
                LOG.debug("have command " + command
                    + " but not compatible with current state " + state
                    + ". Ignoring.");
            }
            break;

          default:
            LOG.debug("notified of an irrelevant command: " + command);
        }
      } catch (IOException e) {
        LOG.error("Error processing host state change!", e);
        return;
      }
    }
  }

  private void update() {
    if (updateThread != null) {
      throw new IllegalStateException("update got called again unexpectedly!");
    }
    Runnable updateRunnable = new Runnable() {
      @Override
      public void run() {
        try {
          IUpdateManager updateManager = getUpdateManager();
          updateManager.update();
          setState(HostState.IDLE);
          updateThread = null;
        } catch (Throwable e) {
          // TODO: should this take the server down?
          LOG.fatal("updater encountered a fatal error!", e);
        }
      }
    };
    updateThread = new Thread(updateRunnable, "update manager thread");
    updateThread.start();
  }

  protected IUpdateManager getUpdateManager() throws DataNotFoundException, IOException {
    return new UpdateManager(configurator, hostConfig, ringGroupConfig, ringConfig);
  }
}
