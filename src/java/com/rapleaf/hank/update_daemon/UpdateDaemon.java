package com.rapleaf.hank.update_daemon;
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

import com.rapleaf.hank.config.DomainConfig;
import com.rapleaf.hank.config.DomainConfigVersion;
import com.rapleaf.hank.config.DomainGroupConfig;
import com.rapleaf.hank.config.PartDaemonAddress;
import com.rapleaf.hank.config.RingConfig;
import com.rapleaf.hank.config.RingGroupConfig;
import com.rapleaf.hank.config.UpdateDaemonConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DaemonState;
import com.rapleaf.hank.coordinator.DaemonType;
import com.rapleaf.hank.coordinator.Coordinator.DaemonStateChangeListener;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.util.HostUtils;

public class UpdateDaemon implements DaemonStateChangeListener {
  private static final Logger LOG = Logger.getLogger(UpdateDaemon.class);

  private static final String HOST_NAME;
  static {
    try {
      HOST_NAME = HostUtils.getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

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

  private final int ringNumber;

  private final String ringGroupName;

  private final PartDaemonAddress hostAddress;

  public UpdateDaemon(UpdateDaemonConfigurator configurator, String hostName) throws UnknownHostException {
    this.configurator = configurator;
    this.coord = configurator.getCoordinator();
    this.ringGroupName = configurator.getRingGroupName();
    this.ringNumber = configurator.getRingNumber();
    hostAddress = new PartDaemonAddress(hostName, configurator.getServicePort());
    coord.addDaemonStateChangeListener(ringGroupName, ringNumber, hostAddress, DaemonType.UPDATE_DAEMON, this);
  }

  void run() throws UnknownHostException {
    goingDown = false;

    // prime things the process by querying the current state and feeding it to
    // the event handler
    DaemonState state = coord.getDaemonState(ringGroupName, ringNumber, hostAddress, DaemonType.UPDATE_DAEMON);
    onDaemonStateChange(ringGroupName, ringNumber, hostAddress, DaemonType.UPDATE_DAEMON, state);

    while (!goingDown) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while waiting for watching thread to go to wake up!", e);
        break;
      }
    }
  }

  @Override
  public void onDaemonStateChange(String ringGroupName, int ringNumber, PartDaemonAddress hostAddress, DaemonType type, DaemonState newState) {
    // we only pay attention to state changes for *this* daemon, so don't need
    // to be concerned with the identification stuff
    switch (newState) {
      case UPDATING:
        // we must have crashed while updating in a prior iteration. just pick
        // up where we left off.
      case UPDATEABLE:
        setUpdating();
        try {
          update();
          setIdle();
        } catch (DataNotFoundException e) {
          LOG.fatal("Caught an unexpected exception during update process!", e);
          goingDown = true;
        }
        break;

      default:
        // we don't care about other state changes, because they're not relevant
        // to us. but let's log them anyways for the sake of completeness
        LOG.info("Notified of uninteresting state change: " + newState + ". Ignoring.");
    }
  }

  private void update() throws DataNotFoundException {
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
      for (Integer part : ringConfig.getDomainPartitionsForHost(hostAddress, domainId)) {
        LOG.debug(String.format("Configuring update task for group-%s/ring-%d/domain-%s/part-%d", ringGroupConfig.getName(), ringConfig.getRingNumber(), domainConfig.getName(), part));
        executor.execute(new UpdateToDo(engine, part, exceptionQueue));
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

  private void setIdle() {
    setState(DaemonState.IDLE);
  }

  private void setUpdating() {
    setState(DaemonState.UPDATING);
  }
  
  private void setState(DaemonState state) {
    coord.setDaemonState(ringGroupName, ringNumber, hostAddress, DaemonType.UPDATE_DAEMON, state);
  }

  /**
   * Main method.
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    String configPath = args[0];
    String log4jprops = args[1];

    PropertyConfigurator.configure(log4jprops);

    new UpdateDaemon(null, HostUtils.getHostName()).run();
  }
}
