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
package com.rapleaf.hank.update_daemon;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.rapleaf.hank.config.UpdateDaemonConfigurator;
import com.rapleaf.hank.config.YamlConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainConfigVersion;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostDomainPartitionConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.coordinator.UpdateDaemonState;
import com.rapleaf.hank.coordinator.HostConfig.HostStateChangeListener;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.util.HostUtils;

public class UpdateDaemon implements HostStateChangeListener {
  private static final Logger LOG = Logger.getLogger(UpdateDaemon.class);

  private final class UpdateToDo implements Runnable {
    private final StorageEngine engine;
    private final int partNum;
    private final Queue<Throwable> exceptionQueue;

    public UpdateToDo(StorageEngine engine, int partNum, Queue<Throwable> exceptionQueue) {
      this.engine = engine;
      this.partNum = partNum;
      this.exceptionQueue = exceptionQueue;
    }

    @Override
    public void run() {
      try {
        LOG.debug(String.format("UpdateToDo %s part %d starting...", engine.toString(), partNum));
        engine.getUpdater(configurator, partNum).update();
        LOG.debug(String.format("UpdateToDo %s part %d completed.", engine.toString(), partNum));
      } catch (Throwable e) {
        // TODO: i just *know* that i'm going to end up wishing i toStringed the
        // storage engine and part num here
        LOG.fatal("Failed to complete an UpdateToDo!", e);
        exceptionQueue.add(e);
      }
    }
  }

  private final UpdateDaemonConfigurator configurator;
  private final Coordinator coord;
  private boolean goingDown;

  private final PartDaemonAddress hostAddress;
  private final HostConfig hostConfig;

  public UpdateDaemon(UpdateDaemonConfigurator configurator, String hostName) throws DataNotFoundException, IOException {
    this.configurator = configurator;
    this.coord = configurator.getCoordinator();
    hostAddress = new PartDaemonAddress(hostName, configurator.getServicePort());
    hostConfig = coord.getRingGroupConfig(configurator.getRingGroupName()).getRingConfigForHost(hostAddress).getHostConfigByAddress(hostAddress);
    hostConfig.setStateChangeListener(this);
  }

  void run() throws UnknownHostException {
    goingDown = false;

    // prime things the process by querying the current state and feeding it to
    // the event handler
    stateChange(hostConfig);

    while (!goingDown) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while waiting for watching thread to go to wake up!", e);
        break;
      }
    }
  }

  private void update() throws DataNotFoundException, IOException {
    ThreadFactory factory = new ThreadFactory() {
      private int x = 0;

      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "Updater Thread Pool Thread #" + ++x);
      }
    };

    ExecutorService executor = new ThreadPoolExecutor(
        configurator.getNumConcurrentUpdates(),
        configurator.getNumConcurrentUpdates(),
        1, TimeUnit.DAYS,
        new LinkedBlockingQueue<Runnable>(),
        factory);
    Queue<Throwable> exceptionQueue = new LinkedBlockingQueue<Throwable>();

    RingGroupConfig ringGroupConfig = coord.getRingGroupConfig(configurator.getRingGroupName());
    RingConfig ringConfig = ringGroupConfig.getRingConfigForHost(hostAddress);

    DomainGroupConfig domainGroupConfig = ringGroupConfig.getDomainGroupConfig();
    for (DomainConfigVersion dcv : domainGroupConfig.getLatestVersion().getDomainConfigVersions()) {
      DomainConfig domainConfig = dcv.getDomainConfig();
      StorageEngine engine = domainConfig.getStorageEngine();

      int domainId = domainGroupConfig.getDomainId(domainConfig.getName());
      for (HostDomainPartitionConfig part : ringConfig.getHostConfigByAddress(hostAddress).getDomainById(domainId).getPartitions()) {
        LOG.debug(String.format("Configuring update task for group-%s/ring-%d/domain-%s/part-%d from %d to %d",
            ringGroupConfig.getName(),
            ringConfig.getRingNumber(),
            domainConfig.getName(),
            part.getPartNum(),
            part.getCurrentDomainGroupVersion(),
            part.getUpdatingToDomainGroupVersion()));
        executor.execute(new UpdateToDo(engine, part.getPartNum(), exceptionQueue));
      }
    }

    try {
      boolean terminated = false;
      executor.shutdown();
      while (!terminated) {
        LOG.debug("Waiting for update executor to complete...");
        terminated = executor.awaitTermination(100, TimeUnit.MILLISECONDS);
        // TODO: report on progress?
        // TODO: check exception queue
      }
    } catch (InterruptedException e) {
      // TODO: log and quit
    }
  }

  private void setIdle() throws IOException {
    setState(UpdateDaemonState.IDLE);
  }

  private void setUpdating() throws IOException {
    setState(UpdateDaemonState.UPDATING);
  }
  
  private void setState(UpdateDaemonState state) throws IOException {
    hostConfig.setUpdateDaemonState(state);
  }

  /**
   * Main method.
   * @param args
   * @throws IOException
   * @throws DataNotFoundException 
   */
  public static void main(String[] args) throws IOException, DataNotFoundException {
    String configPath = args[0];
    String log4jprops = args[1];

    PropertyConfigurator.configure(log4jprops);
    UpdateDaemonConfigurator conf = new YamlConfigurator(configPath);

    new UpdateDaemon(conf, HostUtils.getHostName()).run();
  }

  @Override
  public void stateChange(HostConfig hostConfig) {
    // we only pay attention to state changes for *this* daemon, so don't need
    // to be concerned with the identification stuff
    try {
      final UpdateDaemonState newState = hostConfig.getUpdateDaemonState();
      switch (newState) {
        case UPDATING:
          // we must have crashed while updating in a prior iteration. just pick
          // up where we left off.
        case UPDATABLE:
          setUpdating();
          try {
            update();
            setIdle();
          } catch (DataNotFoundException e) {
            LOG.fatal("Caught an unexpected exception during update process!", e);
            goingDown = true;
          } catch (IOException e) {
            // TODO: hopefully a transient error.
            LOG.error(e);
          }
          break;

        default:
          // we don't care about other state changes, because they're not relevant
          // to us. but let's log them anyways for the sake of completeness
          LOG.info("Notified of uninteresting state change: " + newState + ". Ignoring.");
      }
    } catch (Exception e) {
      LOG.error(e);
    }
  }
}
