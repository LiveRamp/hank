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

import com.rapleaf.hank.config.PartitionServerConfigurator;
import com.rapleaf.hank.config.yaml.YamlPartitionServerConfigurator;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.util.CommandLineChecker;
import com.rapleaf.hank.util.HostUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.THsHaServer.Args;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

/**
 * The main class of the PartitionServer.
 */
public class PartitionServer implements HostCommandQueueChangeListener, HostCurrentCommandChangeListener {

  private static final Logger LOG = Logger.getLogger(PartitionServer.class);
  private static final long MAIN_THREAD_STEP_SLEEP_MS = 1000;

  private final PartitionServerConfigurator configurator;
  private final Coordinator coordinator;
  private Thread dataServerThread;
  private TServer dataServer;
  private boolean stopping = false;
  private boolean hasProcessedCommandOnStartup = false;
  private final PartitionServerAddress hostAddress;
  private final Host host;

  private Thread updateThread;

  private final RingGroup ringGroup;

  private final Ring ring;

  public PartitionServer(PartitionServerConfigurator configurator, String hostName) throws IOException {
    this.configurator = configurator;
    this.coordinator = configurator.createCoordinator();
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
    host.setCurrentCommandChangeListener(this);
  }

  public void run() throws IOException, InterruptedException {
    setStateSynchronized(HostState.IDLE); // In case of exception, server will stop and state will be coherent.

    processCommandOnStartup();
    while (!stopping) {
      try {
        Thread.sleep(MAIN_THREAD_STEP_SLEEP_MS);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted in run loop. Exiting.", e);
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
    return new UpdateManager(configurator, host, ringGroup, ring);
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
  public synchronized void onCurrentCommandChange(Host host) {
    // Do not process anything when stopping
    if (stopping) {
      return;
    }
    try {
      HostCommand command = host.getCurrentCommand();
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
        LOG.debug(ignoreIncompatibleCommandMessage(HostCommand.GO_TO_IDLE, state));
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
        LOG.debug(ignoreIncompatibleCommandMessage(HostCommand.EXECUTE_UPDATE, state));
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
        LOG.debug(ignoreIncompatibleCommandMessage(HostCommand.SERVE_DATA, state));
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
  private void startThriftServer() throws TTransportException, IOException, InterruptedException {
    // set up the service handler
    IfaceWithShutdown handler = getHandler();

    // launch the thrift server
    TNonblockingServerSocket serverSocket = new TNonblockingServerSocket(configurator.getServicePort());
    Args options = new Args(serverSocket);
    options.processor(new com.rapleaf.hank.generated.PartitionServer.Processor(handler));
    options.workerThreads(configurator.getNumThreads());
    options.protocolFactory(new TCompactProtocol.Factory());
    dataServer = new THsHaServer(options);
    LOG.debug("Launching Thrift server...");
    dataServer.serve();
    LOG.debug("Thrift server exited.");
    handler.shutDown();
    LOG.debug("Handler shutdown.");
  }

  /**
   * Start serving the data. Returns when the server is up.
   */
  private void serveData() {
    if (dataServer != null) {
      LOG.info("Data server is already running. Cannot serve data.");
      return;
    }
    Runnable r = new Runnable() {
      @Override
      public void run() {
        try {
          startThriftServer();
        } catch (Exception e) {
          // Data server is probably going down unexpectedly, stop the partition server
          LOG.fatal("Data server thread encountered a fatal exception and is stopping.", e);
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
        LOG.debug("Data server isn't online yet. Waiting...");
        Thread.sleep(1000);
      }
      LOG.info("Data server online and serving.");
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for data server thread to start", e);
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
