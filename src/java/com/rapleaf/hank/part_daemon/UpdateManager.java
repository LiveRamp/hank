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
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.rapleaf.hank.config.PartservConfigurator;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainConfigVersion;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostDomainPartitionConfig;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.storage.StorageEngine;

/**
 * Manages the domain update process.
 */
class UpdateManager implements IUpdateManager {
  private static final Logger LOG = Logger.getLogger(UpdateManager.class);

  private final class UpdateToDo implements Runnable {
    private final StorageEngine engine;
    private final int partNum;
    private final Queue<Throwable> exceptionQueue;
    private final int toVersion;
    private final HostDomainPartitionConfig part;
    private final String domainName;

    public UpdateToDo(StorageEngine engine, int partNum, Queue<Throwable> exceptionQueue, int toVersion, HostDomainPartitionConfig part, String domainName) {
      this.engine = engine;
      this.partNum = partNum;
      this.exceptionQueue = exceptionQueue;
      this.toVersion = toVersion;
      this.part = part;
      this.domainName = domainName;
    }

    @Override
    public void run() {
      try {
        LOG.debug(String.format("%sp%d to version %d starting (%s)", domainName, partNum, toVersion, engine.toString()));
        engine.getUpdater(configurator, partNum).update(toVersion);
        part.setCurrentDomainGroupVersion(toVersion);
        part.setUpdatingToDomainGroupVersion(null);
        LOG.debug(String.format("UpdateToDo %s part %d completed.", engine.toString(), partNum));
      } catch (Throwable e) {
        LOG.fatal("Failed to complete an UpdateToDo!", e);
        exceptionQueue.add(e);
      }
    }
  }

  private final PartservConfigurator configurator;
  private final HostConfig hostConfig;
  private final RingGroupConfig ringGroupConfig;
  private final RingConfig ringConfig;

  public UpdateManager(PartservConfigurator configurator, HostConfig hostConfig, RingGroupConfig ringGroupConfig, RingConfig ringConfig) throws DataNotFoundException, IOException {
    this.configurator = configurator;
    this.hostConfig = hostConfig;
    this.ringGroupConfig = ringGroupConfig;
    this.ringConfig = ringConfig;
  }

  public void update() throws DataNotFoundException, IOException {
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

    DomainGroupConfig domainGroupConfig = ringGroupConfig.getDomainGroupConfig();
    for (DomainConfigVersion dcv : domainGroupConfig.getLatestVersion().getDomainConfigVersions()) {
      DomainConfig domainConfig = dcv.getDomainConfig();
      StorageEngine engine = domainConfig.getStorageEngine();

      int domainId = domainGroupConfig.getDomainId(domainConfig.getName());
      for (HostDomainPartitionConfig part : hostConfig.getDomainById(domainId).getPartitions()) {
        if (part.getUpdatingToDomainGroupVersion() != null) {
          LOG.debug(String.format("Configuring update task for group-%s/ring-%d/domain-%s/part-%d from %d to %d",
              ringGroupConfig.getName(),
              ringConfig.getRingNumber(),
              domainConfig.getName(),
              part.getPartNum(),
              part.getCurrentDomainGroupVersion(),
              part.getUpdatingToDomainGroupVersion()));
          executor.execute(new UpdateToDo(engine,
              part.getPartNum(),
              exceptionQueue,
              dcv.getVersionNumber(),
              part,
              domainConfig.getName()));
        }
      }
    }

    try {
      boolean terminated = false;
      executor.shutdown();
      while (!terminated) {
        LOG.debug("Waiting for update executor to complete...");
        terminated = executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        if (!exceptionQueue.isEmpty()) {
          LOG.fatal("An UpdateToDo encountered an exception:", exceptionQueue.poll());
          throw new RuntimeException("Failed to complete update!");
        }
      }
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while waiting for update to complete. Terminating.");
    }
  }
}
