/**
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
package com.liveramp.hank.partition_server;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.PartitionServerConfigurator;
import com.liveramp.hank.config.yaml.YamlPartitionServerConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostCommand;
import com.liveramp.hank.coordinator.HostCommandQueueChangeListener;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.Hosts;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.generated.ConnectedServerMetadata;
import com.liveramp.hank.generated.HostMetadata;
import com.liveramp.hank.util.CommandLineChecker;
import com.liveramp.hank.util.HankTimer;
import com.liveramp.hank.util.UpdateStatisticsRunnable;
import com.liveramp.hank.zookeeper.WatchedNodeListener;

import static com.liveramp.hank.util.LocalHostUtils.getHostName;

public class PartitionServer implements HostCommandQueueChangeListener, WatchedNodeListener<HostCommand> {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionServer.class);
  private static final long MAIN_THREAD_STEP_SLEEP_MS = 1000;
  private static final int UPDATE_FILESYSTEM_STATISTICS_THREAD_SLEEP_TIME_MS_DEFAULT = 2 * 60 * 1000;

  private static final int HOST_RING_CONNECT_SLEEP_TIME_MS_DEFAULT = 5*1000;

  private static final int NUM_WARMUP_QUERIES_PER_THREAD = 100;

  private static final long MAX_BUFFER_SIZE = 1L << 24; //  16MB

  private final PartitionServerConfigurator configurator;
  private final Coordinator coordinator;

  private final LinkedBlockingQueue<HostCommand> commandQueue;

  private boolean stopping = false;
  private boolean hasProcessedCommandOnStartup = false;
  private final PartitionServerAddress hostAddress;
  private Host host;
  private final String hostName;

  private Thread updateThread;
  private Thread offlineWatcherThread;

  private TThreadedSelectorServer dataServer;
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
    this.hostName = hostName;

  }

  private void connectToRing() throws IOException, InterruptedException {

    ringGroup.registerServer(new ConnectedServerMetadata(hostName,
        configurator.getServicePort(),
        System.currentTimeMillis(),
        configurator.getEnvironmentFlags()
    ));

    Ring ring = null;
    while (!stopping) {
      ring = ringGroup.getRingForHost(hostAddress);

      if(ring != null){
        LOG.info("Found ring for host: "+ring);

        host = ring.getHostByAddress(hostAddress);

        if (host != null){
         LOG.info("Found host info in ring: "+host);
          break;

        }else{
          LOG.info("Could not get host for host address: " + hostAddress
              + " in ring group " + ringGroup.getName() + " ring " + ring.getRingNumber());
        }

      }else{
        LOG.info("Could not get host for host address: " + hostAddress
            + " in ring group " + ringGroup.getName()+".  Sleeping.");
      }

      Thread.sleep(HOST_RING_CONNECT_SLEEP_TIME_MS_DEFAULT);
    }
    if (Hosts.isOnline(host)) {
      throw new RuntimeException("Could not start a partition server for host " + host
          + " since it is already online.");
    }
    host.setCommandQueueChangeListener(this);
    host.setCurrentCommandChangeListener(this);
    host.setEnvironmentFlags(configurator.getEnvironmentFlags());

    // Start the update filesystem statistics thread
    updateFilesystemStatisticsRunnable = new UpdateFilesystemStatisticsRunnable();
    updateFilesystemStatisticsThread = new Thread(updateFilesystemStatisticsRunnable, "Update Filesystem Statistics");
    updateFilesystemStatisticsThread.setDaemon(true);
    updateFilesystemStatisticsThread.start();

  }

  public void run() throws IOException, InterruptedException {

    connectToRing();

    // Add shutdown hook
    addShutdownHook();
    // Initialize and process commands
    setStateSynchronized(HostState.IDLE); // In case of exception, server will stop and state will be coherent.
    // Wait for state to propagate

    addServerOfflineWatcher();

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
    // Disconnect from zookeeper
    coordinator.close();
  }

  private void addServerOfflineWatcher() {
    Runnable serverOfflineWatcher = new Runnable() {
      @Override
      public void run() {
        //TODO make these configurable
        long units = 10l;
        TimeUnit unit = TimeUnit.MINUTES;
        try {
          while (true) {
            HostState state = getStateSafe();
            LOG.info("Current state: "+state);
            if (state == null || HostState.OFFLINE.equals(state)) {
              LOG.info("OFFLINE.  Starting shutdown countdown");
              startShutDownCountdown(units, unit);
            }
            unit.sleep(units);
          }
        }catch (Exception e){
          LOG.error("Watcher thread encountered an error - thread will stop for safety", e);
        }
      }

      public HostState getStateSafe() {
        HostState state;
        try {
          state = host.getState();
        } catch (IOException e) {
          LOG.error("Offline watcher failed to get state, counting as OFFLINE. Exception: ", e);
          state = HostState.OFFLINE;
        }
        return state;
      }

      private void startShutDownCountdown(long units, TimeUnit unit) {
        try {
          unit.sleep(units);
          HostState state = getStateSafe();
          if (state == null || HostState.OFFLINE.equals(state)) {
            LOG.error("Partition Server was OFFLINE for " + units + " " + unit.toString());
            stopSynchronized();
          }else{
            LOG.error("Shutdown cancelled, state is currently: " + state);
          }
        } catch (InterruptedException e) {
          LOG.error("Interrupted while performing shutdown countdown", e);
        }
      }
    };

    offlineWatcherThread = new Thread(serverOfflineWatcher, "Server Offline Watcher");
    offlineWatcherThread.setDaemon(true);
    offlineWatcherThread.start();
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
        LOG.info(ignoreIncompatibleCommandMessage(HostCommand.GO_TO_IDLE, state));
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
        LOG.info(ignoreIncompatibleCommandMessage(HostCommand.EXECUTE_UPDATE, state));
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
        LOG.info(ignoreIncompatibleCommandMessage(HostCommand.SERVE_DATA, state));
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
          LOG.error("Update failed. Updater encountered a fatal error:", e);
          try {

            long cooldown = configurator.getUpdateFailureCooldown();

            LOG.error("Will retry update in "+cooldown+ "ms.");
            Thread.sleep(cooldown);

          } catch (InterruptedException e1) {
            //  no op
          }
        }
        // Go back to IDLE even in case of failure
        try {
          setStateSynchronized(HostState.IDLE); // In case of exception, server will stop and state will be coherent.
        } catch (IOException e) {
          LOG.error("Failed to record state change.", e);
        }
        // Move on to next command
        try {
          nextCommandSynchronized(); // In case of exception, server will stop and state will be coherent.
        } catch (IOException e) {
          LOG.error("Failed to move on to next command.", e);
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
      TThreadedSelectorServer.Args options = new TThreadedSelectorServer.Args(serverSocket);
      options.processor(new com.liveramp.hank.generated.PartitionServer.Processor(handler));
      options.workerThreads(configurator.getNumConcurrentQueries());
      options.selectorThreads(4);
      options.protocolFactory(new TCompactProtocol.Factory());
      options.maxReadBufferBytes = MAX_BUFFER_SIZE;
      dataServer = new TThreadedSelectorServer(options);
      LOG.info("Launching Thrift server.");
      dataServer.serve();
      LOG.info("Thrift server exited.");
      // The Thrift server does not clean up selectors after stopping, which leads to a file descriptor leak.
      // See https://issues.apache.org/jira/browse/THRIFT-2274
      // TODO: when the bug is fixed in Thrift, remove this ugly hack which takes care of the issue
      List<Selector> selectors = getServerSelectors(dataServer);
      closeServerSelectors(selectors);
      // Close the socket
      serverSocket.close();
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
          LOG.error("Data server thread encountered a fatal throwable and is stopping.", t);
          // Stop waiting for data server
          waitForDataServer = false;
          // Stop partition server main thread
          stopSynchronized();
        }
      }
    };
    dataServerThread = new Thread(r, "PartitionServer Thrift data server thread");
    LOG.info("Launching data server thread.");
    dataServerThread.start();
    try {
      while (dataServer == null || !dataServer.isServing()) {
        if (!waitForDataServer) {
          LOG.info("Data server encountered an error. Stop waiting for it to start.");
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
      LOG.error("Interrupted while waiting for data server thread to stop. Continuing.", e);
    }
    dataServer = null;
    dataServerThread = null;
    LOG.info("Data server thread stopped");
  }

  private String ignoreIncompatibleCommandMessage(HostCommand command, HostState state) {
    return String.format("Ignoring command %s because it is incompatible with state %s.", command, state);
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
      Hosts.setFilesystemStatistics(host, getFilesystemStatistics());
    }

    @Override
    protected void cleanup() {
      try {
        Hosts.deleteFilesystemStatistics(host);
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

  static List<Selector> getServerSelectors(TThreadedSelectorServer server) {
    List<Selector> result = new ArrayList<Selector>();
    try {
      // Get accept thread selector
      Field acceptThreadField = server.getClass().getDeclaredField("acceptThread");
      acceptThreadField.setAccessible(true);
      Thread acceptThread = (Thread)acceptThreadField.get(server);
      Field acceptSelectorField = acceptThread.getClass().getDeclaredField("acceptSelector");
      acceptSelectorField.setAccessible(true);
      Selector acceptSelector = (Selector)acceptSelectorField.get(acceptThread);
      result.add(acceptSelector);
      // Get the other selectors
      Field selectorThreadField = server.getClass().getDeclaredField("selectorThreads");
      selectorThreadField.setAccessible(true);
      Set selectorThreads = (Set)selectorThreadField.get(server);
      for (Object selectorThread : selectorThreads) {
        Field selectorThreadSelectorField = selectorThread.getClass().getSuperclass().getDeclaredField("selector");
        selectorThreadSelectorField.setAccessible(true);
        Selector selector = (Selector)selectorThreadSelectorField.get(selectorThread);
        result.add(selector);
      }
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  static void closeServerSelectors(List<Selector> selectors) {
    for (Selector selector : selectors) {
      try {
        selector.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
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
      TSocket socket = null;
      TFramedTransport transport = null;
      try {
        socket = new TSocket(host.getAddress().getHostName(), host.getAddress().getPortNumber(), 0);
        transport = new TFramedTransport(socket);
        transport.open();
        TProtocol proto = new TCompactProtocol(transport);
        com.liveramp.hank.generated.PartitionServer.Client client = new com.liveramp.hank.generated.PartitionServer.Client(proto);
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
        if (socket != null) {
          socket.close();
        }
      }
    }
  }
}
