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

import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.config.PartitionServerConfigurator;
import com.rapleaf.hank.config.yaml.YamlPartitionServerConfigurator;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.util.CommandLineChecker;
import com.rapleaf.hank.zookeeper.WatchedNodeListener;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.THsHaServer.Args;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

import static com.rapleaf.hank.util.LocalHostUtils.getHostName;

/**
 * The main class of the PartitionServer.
 */
public class PartitionServer implements HostCommandQueueChangeListener, WatchedNodeListener<HostCommand> {

  private static final Logger LOG = Logger.getLogger(PartitionServer.class);
  private static final long MAIN_THREAD_STEP_SLEEP_MS = 1000;

  private final PartitionServerConfigurator configurator;
  private final Coordinator coordinator;

  private boolean stopping = false;
  private boolean hasProcessedCommandOnStartup = false;
  private final PartitionServerAddress hostAddress;
  private final Host host;

  private Thread updateThread;

  private TServer dataServer;
  private Thread dataServerThread;
  private boolean waitForDataServer;

  private final RingGroup ringGroup;

  private Thread shutdownHook;

  public PartitionServer(PartitionServerConfigurator configurator, String hostName) throws IOException {
    this.configurator = configurator;
    this.coordinator = configurator.createCoordinator();
    hostAddress = new PartitionServerAddress(hostName, configurator.getServicePort());
    ringGroup = coordinator.getRingGroup(configurator.getRingGroupName());
    if (ringGroup == null) {
      throw new RuntimeException("Could not get ring group: " + configurator.getRingGroupName());
    }
    Ring ring = ringGroup.getRingForHost(hostAddress);
    if (ring == null) {
      throw new RuntimeException("Could not get ring for host address: " + hostAddress
          + " in ring group " + ringGroup.getName());
    }
    host = ring.getHostByAddress(hostAddress);
    if (host == null) {
      throw new RuntimeException("Could not get host for host address: " + hostAddress
          + " in ring group " + ringGroup.getName() + " ring " + ring.getRingNumber());
    }
    if (Hosts.isOnline(host)) {
      throw new RuntimeException("Could not start a partition server for host " + host
          + " since it is already online.");
    }
    host.setCommandQueueChangeListener(this);
    host.setCurrentCommandChangeListener(this);
  }

  public void run() throws IOException, InterruptedException {
    // Add shutdown hook
    addShutdownHook();
    // Initialize and process commands
    setStateSynchronized(HostState.IDLE); // In case of exception, server will stop and state will be coherent.
    // Wait for state to propagate
    while (host.getState() != HostState.IDLE) {
      LOG.info("Waiting for Host state " + HostState.IDLE + " to propagate.");
      Thread.sleep(100);
    }
    processCommandOnStartup();
    while (!stopping) {
      try {
        Thread.sleep(MAIN_THREAD_STEP_SLEEP_MS);
      } catch (InterruptedException e) {
        LOG.info("Interrupted in run loop. Exiting.", e);
        break;
      }
    }
    // Shuting down
    LOG.info("Partition server main thread is stopping.");
    // Stop serving data
    stopServingData();
    // Stop updating if necessary
    if (updateThread != null) {
      LOG.info("Update thread is still running. Interrupting and waiting for it to finish...");
      updateThread.interrupt();
      updateThread.join(); // In case of interrupt exception, server will stop and state will be coherent.
    }
    setStateSynchronized(HostState.OFFLINE); // In case of exception, server will stop and state will be coherent.
    // Remove shutdown hook. We don't need it anymore as we just set the host state to OFFLINE
    removeShutdownHook();
  }

  // Stop the partition server. Can be called from another thread.
  public synchronized void stopSynchronized() {
    stop();
  }

  // Stop the partition server
  private void stop() {
    stopping = true;
  }

  protected IfaceWithShutdown getHandler() throws IOException {
    return new PartitionServerHandler(hostAddress, configurator, coordinator);
  }

  protected IUpdateManager getUpdateManager() throws IOException {
    return new UpdateManager(configurator, host, ringGroup);
  }

  @Override
  public synchronized void onCommandQueueChange(Host host) {
    // Do not process anything when we have not yet tried to process a command when starting up.
    if (!hasProcessedCommandOnStartup) {
      return;
    }
    try {
      HostCommand command = host.getCurrentCommand();
      if (command == null) {
        // When command queue changes, and current command is empty, move on to next command
        host.nextCommand();
      } else {
        // A current command was already in place, we are still processing it. Do nothing.
      }
    } catch (IOException e) {
      LOG.error("Failed to move on to next command.", e);
      stop();
    }
  }

  @Override
  public synchronized void onWatchedNodeChange(HostCommand command) {
    // Do not process anything when stopping
    if (stopping) {
      LOG.error("Ignoring command " + command + " because server is stopping.");
      return;
    }
    try {
      if (command != null) {
        processCommand(command, host.getState());
      }
    } catch (IOException e) {
      LOG.error("Failed to process current command.", e);
      stop();
    }
  }

  public synchronized void processCommandOnStartup() {
    try {
      HostCommand command = host.getCurrentCommand();
      if (command != null) {
        processCommand(command, host.getState());
      } else {
        host.nextCommand();
      }
    } catch (IOException e) {
      LOG.error("Failed to process current command on startup.", e);
      stop();
    }
    hasProcessedCommandOnStartup = true;
  }

  private synchronized void setStateSynchronized(HostState state) throws IOException {
    // In case of failure to set host state, stop the partition server and rethrow the exception.
    try {
      host.setState(state);
    } catch (IOException e) {
      stop();
      throw e;
    }
  }

  private synchronized HostCommand nextCommandSynchronized() throws IOException {
    // In case of failure to move on to next command, stop the partition server and rethrow the exception.
    try {
      return host.nextCommand();
    } catch (IOException e) {
      stop();
      throw e;
    }
  }

  private void processCommand(HostCommand command, HostState state) throws IOException {
    LOG.info("Processing command: " + command);
    switch (command) {
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

  private void processGoToIdle(HostState state) throws IOException {
    switch (state) {
      case SERVING:
        stopServingData();
        host.setState(HostState.IDLE); // In case of exception, server will stop and state will be coherent.
        host.nextCommand(); // In case of exception, server will stop and state will be coherent.
        break;
      default:
        if (LOG.isDebugEnabled()) {
          LOG.info(ignoreIncompatibleCommandMessage(HostCommand.GO_TO_IDLE, state));
        }
        host.nextCommand(); // In case of exception, server will stop and state will be coherent.
    }
  }

  private void processExecuteUpdate(HostState state) throws IOException {
    switch (state) {
      case IDLE:
        host.setState(HostState.UPDATING); // In case of exception, server will stop and state will be coherent.
        executeUpdate();
        // Next command is set by the updater thread
        break;
      default:
        if (LOG.isDebugEnabled()) {
          LOG.info(ignoreIncompatibleCommandMessage(HostCommand.EXECUTE_UPDATE, state));
        }
        host.nextCommand(); // In case of exception, server will stop and state will be coherent.
    }
  }

  private void processServeData(HostState state) throws IOException {
    switch (state) {
      case IDLE:
        serveData();
        host.setState(HostState.SERVING);  // In case of exception, server will stop and state will be coherent.
        host.nextCommand(); // In case of exception, server will stop and state will be coherent.
        break;
      default:
        if (LOG.isDebugEnabled()) {
          LOG.info(ignoreIncompatibleCommandMessage(HostCommand.SERVE_DATA, state));
        }
        host.nextCommand(); // In case of exception, server will stop and state will be coherent.
    }
  }

  private void executeUpdate() {
    if (updateThread != null) {
      LOG.error("Update got called while one is already running!");
      return;
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
        // Go back to IDLE even in case of failure
        try {
          setStateSynchronized(HostState.IDLE); // In case of exception, server will stop and state will be coherent.
        } catch (IOException e) {
          LOG.fatal("Failed to record state change.", e);
        }
        // Move on to next command
        try {
          nextCommandSynchronized(); // In case of exception, server will stop and state will be coherent.
        } catch (IOException e) {
          LOG.fatal("Failed to move on to next command.", e);
        }
        // Signal that update thread is done.
        updateThread = null;
      }
    };
    updateThread = new Thread(updateRunnable, "Update manager thread");
    updateThread.start();
  }

  /**
   * Start serving the thrift server. doesn't return.
   *
   * @throws TTransportException
   * @throws IOException
   * @throws InterruptedException
   */
  protected void startThriftServer() throws TTransportException, IOException, InterruptedException {
    IfaceWithShutdown handler = null;
    try {
      // Set up the service handler
      handler = getHandler();
      // Launch the thrift server
      TNonblockingServerSocket serverSocket = new TNonblockingServerSocket(configurator.getServicePort());
      Args options = new Args(serverSocket);
      options.processor(new com.rapleaf.hank.generated.PartitionServer.Processor(handler));
      options.workerThreads(configurator.getNumConcurrentQueries());
      options.protocolFactory(new TCompactProtocol.Factory());
      dataServer = new THsHaServer(options);
      LOG.debug("Launching Thrift server...");
      dataServer.serve();
      LOG.debug("Thrift server exited.");
    } finally {
      // Always shut down the handler
      if (handler != null) {
        LOG.debug("Shutting down Partition Server handler.");
        handler.shutDown();
      }
    }
  }

  /**
   * Start serving the data. Returns when the server is up.
   */
  private void serveData() throws IOException {
    waitForDataServer = true;
    if (dataServer != null) {
      LOG.info("Data server is already running. Cannot serve data.");
      return;
    }
    Runnable r = new Runnable() {
      @Override
      public void run() {
        try {
          startThriftServer();
        } catch (Throwable t) {
          // Data server is probably going down unexpectedly, stop the partition server
          LOG.fatal("Data server thread encountered a fatal throwable and is stopping.", t);
          // Stop waiting for data server
          waitForDataServer = false;
          // Stop partition server main thread
          stopSynchronized();
        }
      }
    };
    dataServerThread = new Thread(r, "PartitionServer Thrift data server thread");
    LOG.info("Launching data server thread...");
    dataServerThread.start();
    try {
      while (dataServer == null || !dataServer.isServing()) {
        if (!waitForDataServer) {
          LOG.info("Data server encountered an error. Stopping to wait for it to start.");
          break;
        }
        LOG.debug("Data server isn't online yet. Waiting...");
        Thread.sleep(1000);
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for data server thread to start", e);
    }
    if (dataServer == null || !dataServer.isServing()) {
      throw new IOException("Failed to start data server");
    } else {
      LOG.info("Data server online and serving.");
    }
  }

  /**
   * Block until thrift server is down
   */
  private void stopServingData() {
    if (dataServer == null) {
      return;
    }
    LOG.info("Stopping data server thread.");
    dataServer.stop();
    try {
      dataServerThread.join();
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while waiting for data server thread to stop. Continuing.", e);
    }
    dataServer = null;
    dataServerThread = null;
  }

  private String ignoreIncompatibleCommandMessage(HostCommand command, HostState state) {
    return String.format("Ignoring command %s because it is incompatible with state %s.", command, state);
  }

  // Set the host to OFFLINE on VM shutdown
  private void addShutdownHook() {
    if (shutdownHook == null) {
      shutdownHook = new Thread() {
        @Override
        public void run() {
          try {
            if (host != null) {
              host.setState(HostState.OFFLINE);
            }
          } catch (IOException e) {
            // When VM is exiting and we fail to set host to OFFLINE, swallow the exception
          }
        }
      };
      Runtime.getRuntime().addShutdownHook(shutdownHook);
    }
  }

  private void removeShutdownHook() {
    if (shutdownHook != null) {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    }
  }

  public static void main(String[] args) throws IOException, InvalidConfigurationException, InterruptedException {
    CommandLineChecker.check(args, new String[]{"configuration_file_path", "log4j_properties_file_path"},
        PartitionServer.class);
    String configPath = args[0];
    String log4jprops = args[1];

    PartitionServerConfigurator configurator = new YamlPartitionServerConfigurator(configPath);
    PropertyConfigurator.configure(log4jprops);

    new PartitionServer(configurator, getHostName()).run();
  }
}
