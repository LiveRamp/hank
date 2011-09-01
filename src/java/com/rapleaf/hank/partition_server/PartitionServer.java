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
package com.rapleaf.hank.partition_server;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.THsHaServer.Args;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.rapleaf.hank.config.PartitionServerConfigurator;
import com.rapleaf.hank.config.yaml.YamlPartitionServerConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.HostCommandQueueChangeListener;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.PartitionServerAddress;
import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.util.CommandLineChecker;
import com.rapleaf.hank.util.HostUtils;

/**
 * The main class of the PartitionServer.
 */
public class PartitionServer implements HostCommandQueueChangeListener {
  private static final Logger LOG = Logger.getLogger(PartitionServer.class);

  private final PartitionServerConfigurator configurator;
  private Thread serverThread;
  private TServer server;
  private boolean goingDown = false;
  private final PartitionServerAddress hostAddress;
  private final Host host;

  private Thread updateThread;

  private final RingGroup ringGroup;

  private final Ring ring;

  public PartitionServer(PartitionServerConfigurator configurator, String hostName) throws IOException {
    this.configurator = configurator;
    Coordinator coordinator = configurator.getCoordinator();
    hostAddress = new PartitionServerAddress(hostName, configurator.getServicePort());
    ringGroup = coordinator.getRingGroup(configurator.getRingGroupName());
    ring = ringGroup.getRingForHost(hostAddress);
    if (ring == null) {
      throw new RuntimeException("Could not get ring configuration for host: " + hostAddress);
    }
    host = ring.getHostByAddress(hostAddress);
    if (host == null) {
      throw new RuntimeException("Could not get host configuration for host: " + hostAddress);
    }
    host.setCommandQueueChangeListener(this);
  }

  public void run() throws IOException {
    setState(HostState.IDLE);

    processCommands();
    while (!goingDown) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted in run loop. Exiting.");
        break;
      }
    }
    setState(HostState.OFFLINE);
  }

  public void stop() throws IOException {
    // don't wait to be started again.
    goingDown = true;
    stopServingData();
    setState(HostState.IDLE);
  }

  @Override
  public void onCommandQueueChange(Host host) {
    processCommands();
  }

  protected IfaceWithShutdown getHandler() throws IOException {
    return new PartitionServerHandler(hostAddress, configurator);
  }

  protected IUpdateManager getUpdateManager() throws IOException {
    return new UpdateManager(configurator, host, ringGroup, ring);
  }

  private synchronized void processCommands() {
    try {
      if (host.getCurrentCommand() != null) {
        processCurrentCommand(host, host.getCurrentCommand());
      }
      while (!host.getCommandQueue().isEmpty()) {
        HostCommand nextCommand = host.processNextCommand();
        processCurrentCommand(host, nextCommand);
      }
    } catch (IOException e) {
      // TODO
      LOG.error("Uh oh, failed to process all the commands in the queue, somehow...", e);
    }
  }

  /**
   * Start serving the thrift server. doesn't return.
   *
   * @throws TTransportException
   * @throws IOException
   * @throws InterruptedException
   */
  private void startThriftServer() throws TTransportException, IOException, InterruptedException {
    // set up the service handler
    IfaceWithShutdown handler = getHandler();

    // launch the thrift server
    TNonblockingServerSocket serverSocket = new TNonblockingServerSocket(configurator.getServicePort());
    Args options = new Args(serverSocket);
    options.processor(new com.rapleaf.hank.generated.PartitionServer.Processor(handler));
    options.workerThreads(configurator.getNumThreads());
    options.protocolFactory(new TCompactProtocol.Factory());
    server = new THsHaServer(options);
    LOG.debug("Launching Thrift server...");
    server.serve();
    LOG.debug("Thrift server exited.");
    handler.shutDown();
    LOG.debug("Handler shutdown.");
  }

  /**
   * Start serving the data. Returns when the server is up.
   */
  private void serveData() {
    if (server == null) {
      Runnable r = new Runnable() {
        @Override
        public void run() {
          try {
            startThriftServer();
          } catch (Exception e) {
            // TODO deal with exception. server is probably going down unexpectedly
            LOG.fatal("PartitionServer server thread encountered a fatal exception.", e);
          }
        }
      };
      serverThread = new Thread(r, "PartitionServer Thrift Server thread");
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
   * Block until thrift server is down
   */
  private void stopServingData() {
    if (server == null) {
      return;
    }

    server.stop();
    try {
      serverThread.join();
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while waiting for server thread to stop. Continuing.", e);
    }
    server = null;
    serverThread = null;
  }

  private synchronized void setState(HostState state) throws IOException {
    host.setState(state);
  }

  private synchronized void completeCommand() throws IOException {
    host.completeCommand();
  }

  private synchronized void clearCommandQueue() throws IOException {
    host.clearCommandQueue();
  }

  private void update() {
    if (updateThread != null) {
      throw new IllegalStateException("Update got called again unexpectedly!");
    }
    Runnable updateRunnable = new Runnable() {
      @Override
      public void run() {
        try {
          IUpdateManager updateManager = getUpdateManager();
          updateManager.update();
          LOG.info("Update succeeded.");
        } catch (Throwable e) {
          LOG.fatal("Update failed. Updater encountered a fatal error:", e);
        }
        try {
          setState(HostState.IDLE);
          completeCommand();
        } catch (IOException e) {
          LOG.fatal("Updater encountered an error while recording state changes.");
        }
        updateThread = null;
      }
    };
    updateThread = new Thread(updateRunnable, "Update manager thread");
    updateThread.start();
  }

  private void processCurrentCommand(Host host, HostCommand nextCommand) throws IOException {
    HostState state = host.getState();
    switch (nextCommand) {
      case EXECUTE_UPDATE:
        processExecuteUpdate(state);
        break;
      case GO_TO_IDLE:
        processGoToIdle(state);
        break;
      case SERVE_DATA:
        processServeData(state);
        break;
    }
  }

  private void processServeData(HostState state) throws IOException {
    switch (state) {
      case IDLE:
        serveData();
        setState(HostState.SERVING);
        host.completeCommand();
        break;
      default:
        LOG.debug("received command " + HostCommand.SERVE_DATA
            + " but not compatible with current state " + state
            + ". Ignoring.");
    }
  }

  private void processGoToIdle(HostState state) throws IOException {
    switch (state) {
      case SERVING:
        stopServingData();
        setState(HostState.IDLE);
        host.completeCommand();
        break;
      case UPDATING:
        LOG.debug("received command " + HostCommand.GO_TO_IDLE
            + " but current state is " + state
            + ", which cannot be stopped. Will wait until completion.");
        break;
      default:
        LOG.debug("received command " + HostCommand.GO_TO_IDLE
            + " but not compatible with current state " + state
            + ". Ignoring.");
    }
  }

  private void processExecuteUpdate(HostState state) throws IOException {
    switch (state) {
      case IDLE:
        setState(HostState.UPDATING);
        update();
        break;
      case SERVING:
        LOG.debug("Going directly from SERVING to UPDATING is not currently supported.");
      default:
        LOG.debug("have command " + HostCommand.EXECUTE_UPDATE
            + " but not compatible with current state " + state
            + ". Ignoring.");
    }
  }

  public static void main(String[] args) throws Throwable {
    try {
      CommandLineChecker.check(args, new String[]{"configuration_file_path", "log4j_properties_file_path"}, PartitionServer.class);
      String configPath = args[0];
      String log4jprops = args[1];

      PartitionServerConfigurator configurator = new YamlPartitionServerConfigurator(configPath);
      PropertyConfigurator.configure(log4jprops);

      new PartitionServer(configurator, HostUtils.getHostName()).run();
    } catch (Throwable t) {
      System.err.println("usage: bin/start_partition_server.sh <path to config.yml> <path to log4j properties>");
      throw t;
    }
  }
}
