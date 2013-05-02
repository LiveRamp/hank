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
import com.rapleaf.hank.performance.HankTimer;
import com.rapleaf.hank.util.CommandLineChecker;
import com.rapleaf.hank.zookeeper.WatchedNodeListener;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.THsHaServer.Args;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.rapleaf.hank.util.LocalHostUtils.getHostName;

public class PartitionServer implements HostCommandQueueChangeListener, WatchedNodeListener<HostCommand> {

  private static final Logger LOG = Logger.getLogger(PartitionServer.class);
  private static final long MAIN_THREAD_STEP_SLEEP_MS = 1000;
  private static final int UPDATE_FILESYSTEM_STATISTICS_THREAD_SLEEP_TIME_MS_DEFAULT = 2 * 60 * 1000;
  private static final String FILESYSTEM_STATISTICS_KEY = "filesystem_statistics";

  private static final int NUM_WARMUP_QUERIES_PER_THREAD = 1000;

  private final PartitionServerConfigurator configurator;
  private final Coordinator coordinator;

  private final LinkedBlockingQueue<HostCommand> commandQueue;

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
  private UpdateFilesystemStatisticsRunnable updateFilesystemStatisticsRunnable;
  private Thread updateFilesystemStatisticsThread;

  public PartitionServer(PartitionServerConfigurator configurator, String hostName) throws IOException {
    this.configurator = configurator;
    this.coordinator = configurator.createCoordinator();
    this.commandQueue = new LinkedBlockingQueue<HostCommand>();
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

    // Start the update filesystem statistics thread
    updateFilesystemStatisticsRunnable = new UpdateFilesystemStatisticsRunnable();
    updateFilesystemStatisticsThread = new Thread(updateFilesystemStatisticsRunnable, "Update Filesystem Statistics");
    updateFilesystemStatisticsThread.setDaemon(true);
    updateFilesystemStatisticsThread.start();
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
        HostCommand command = commandQueue.poll(MAIN_THREAD_STEP_SLEEP_MS, TimeUnit.MILLISECONDS);
        if (command != null) {
          try {
            processCommand(command, host.getState());
          } catch (IOException e) {
            LOG.error("Failed to process command: " + command, e);
            break;
          }
        }
      } catch (InterruptedException e) {
        LOG.info("Interrupted in main loop. Exiting.", e);
        break;
      }
    }
    // Shuting down
    LOG.info("Partition server main thread is stopping.");
    // Stop serving data
    stopServingData();
    // Stop updating if necessary
    stopUpdating();
    // Signal OFFLINE
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
    LOG.info("Command queue changed.");
    // Do not process anything when we have not yet tried to process a command when starting up.
    if (!hasProcessedCommandOnStartup) {
      LOG.info("Ignoring command queue change as commands have not yet been executed on startup.");
      return;
    }
    try {
      HostCommand command = host.getCurrentCommand();
      if (command == null) {
        // When command queue changes, and current command is empty, move on to next command
        host.nextCommand();
      } else {
        // A current command was already in place, we are still executing it. Do nothing.
        LOG.info("Ignoring command queue change as a command is currently being executed.");
      }
    } catch (IOException e) {
      LOG.error("Failed to move on to next command.", e);
      stop();
    }
  }

  @Override
  public void onWatchedNodeChange(final HostCommand command) {
    LOG.info("Current command changed: " + command);
    // Do not process anything when stopping
    if (stopping) {
      LOG.error("Ignoring command " + command + " because server is stopping.");
      return;
    }
    try {
      if (command != null) {
        commandQueue.put(command);
      }
    } catch (InterruptedException e) {
      LOG.error("Failed to process command.", e);
      stopSynchronized();
    }
  }

  public void processCommandOnStartup() {
    try {
      HostCommand command = host.getCurrentCommand();
      LOG.info("Current command is: " + command);
      if (command != null) {
        commandQueue.put(command);
      } else {
        host.nextCommand();
      }
    } catch (Exception e) {
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
        // Set IDLE state proactively. If the shutting down hangs, it will be safe since clients will be already aware.
        host.setState(HostState.IDLE); // In case of exception, server will stop and state will be coherent.
        stopServingData();
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

  private void stopUpdating() throws InterruptedException {
    if (updateThread != null) {
      LOG.info("Update thread is still running. Interrupting and waiting for it to finish...");
      updateThread.interrupt();
      updateThread.join(); // In case of interrupt exception, server will stop and state will be coherent.
    }
  }

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
      warmUp();
      LOG.info("Data server online and serving.");
    }
  }

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

  public static Map<String, FilesystemStatisticsAggregator> getFilesystemStatistics(Host host) throws IOException {
    String filesystemsStatistics = host.getStatistic(FILESYSTEM_STATISTICS_KEY);

    if (filesystemsStatistics == null) {
      return Collections.emptyMap();
    } else {
      TreeMap<String, FilesystemStatisticsAggregator> result = new TreeMap<String, FilesystemStatisticsAggregator>();
      String[] filesystemStatistics = filesystemsStatistics.split("\n");
      for (String statistics : filesystemStatistics) {
        if (statistics.length() == 0) {
          continue;
        }
        String[] tokens = statistics.split(" ");
        String filesystemRoot = tokens[0];
        long totalSpace = Long.parseLong(tokens[1]);
        long usableSpace = Long.parseLong(tokens[2]);
        result.put(filesystemRoot, new FilesystemStatisticsAggregator(totalSpace, usableSpace));
      }
      return result;
    }
  }

  public static void setFilesystemStatistics(Host host,
                                             Map<String, FilesystemStatisticsAggregator> filesystemsStatistics) throws IOException {
    StringBuilder statistics = new StringBuilder();
    for (Map.Entry<String, FilesystemStatisticsAggregator> entry : filesystemsStatistics.entrySet()) {
      statistics.append(entry.getKey());
      statistics.append(' ');
      statistics.append(entry.getValue().toString());
      statistics.append('\n');
    }
    host.setEphemeralStatistic(FILESYSTEM_STATISTICS_KEY, statistics.toString());
  }

  private Map<String, FilesystemStatisticsAggregator> getFilesystemStatistics() throws IOException {
    Map<String, FilesystemStatisticsAggregator> result = new HashMap<String, FilesystemStatisticsAggregator>();
    for (String filesystemRoot : getUsedFilesystemRoots()) {
      File filesystemRootFile = new File(filesystemRoot);
      result.put(filesystemRoot, new FilesystemStatisticsAggregator(filesystemRootFile.getTotalSpace(), filesystemRootFile.getUsableSpace()));
    }
    return result;
  }

  private Set<String> getUsedFilesystemRoots() throws IOException {
    return configurator.getDataDirectories();
    /*
    // Create set of system roots
    Set<String> filesystemRoots = new HashSet<String>();
    for (File root : File.listRoots()) {
      filesystemRoots.add(root.getCanonicalPath());
    }
    // Determine set of used roots
    Set<String> result = new HashSet<String>();
    for (String dataDirectoryPath : configurator.getDataDirectories()) {
      String dataDirectoryCanonicalPath = new File(dataDirectoryPath).getCanonicalPath();
      String bestFilesystemRoot = null;
      for (String filesystemRoot : filesystemRoots) {
        if (dataDirectoryCanonicalPath.startsWith(filesystemRoot)
            && (bestFilesystemRoot == null || bestFilesystemRoot.length() < filesystemRoot.length())) {
          bestFilesystemRoot = filesystemRoot;
        }
      }
      if (bestFilesystemRoot == null) {
        throw new RuntimeException("Unable to determine filesystem root for directory: " + dataDirectoryCanonicalPath);
      }
      result.add(bestFilesystemRoot);
    }
    return result;
    */
  }

  private void warmUp() throws IOException {
    LOG.info("Warming up data server...");
    List<Thread> threads = new ArrayList<Thread>();
    for (int i = 0; i < configurator.getNumConcurrentQueries(); ++i) {
      threads.add(new Thread(new WarmupRunnable(), "Warmup Thread #" + i));
    }
    HankTimer timer = new HankTimer();
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        LOG.error("Failed to warm up data server", e);
        throw new IOException("Failed to warm up data server", e);
      }
    }
    long warmupDurationMs = timer.getDurationMs();
    LOG.info("Warming up data server took " + warmupDurationMs + " ms");
  }

  /**
   * This thread periodically updates statistics of the Host
   */
  private class UpdateFilesystemStatisticsRunnable extends UpdateStatisticsRunnable implements Runnable {

    public UpdateFilesystemStatisticsRunnable() {
      super(UPDATE_FILESYSTEM_STATISTICS_THREAD_SLEEP_TIME_MS_DEFAULT);
    }

    @Override
    public void runCore() throws IOException {
      setFilesystemStatistics(host, getFilesystemStatistics());
    }

    @Override
    protected void cleanup() {
      try {
        host.deleteStatistic(FILESYSTEM_STATISTICS_KEY);
      } catch (IOException e) {
        LOG.error("Error while deleting runtime statistics.", e);
        throw new RuntimeException(e);
      }
    }
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

  private class WarmupRunnable implements Runnable {

    @Override
    public void run() {
      TFramedTransport transport = null;
      try {
        transport = new TFramedTransport(new TSocket(host.getAddress().getHostName(),
            host.getAddress().getPortNumber(), 0));
        transport.open();
        TProtocol proto = new TCompactProtocol(transport);
        com.rapleaf.hank.generated.PartitionServer.Client client = new com.rapleaf.hank.generated.PartitionServer.Client(proto);
        // Perform queries
        for (int i = 0; i < NUM_WARMUP_QUERIES_PER_THREAD; i++) {
          client.get(0, ByteBuffer.wrap(new byte[0]));
        }
      } catch (TException e) {
        LOG.error("Failed to warm up data server", e);
        throw new RuntimeException("Failed to warm up data server", e);
      } finally {
        if (transport != null) {
          transport.close();
        }
      }
    }
  }
}
