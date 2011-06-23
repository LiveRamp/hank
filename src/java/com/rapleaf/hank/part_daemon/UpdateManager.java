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

import com.rapleaf.hank.config.PartservConfigurator;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.storage.Deleter;
import com.rapleaf.hank.storage.StorageEngine;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Manages the domain update process.
 */
class UpdateManager implements IUpdateManager {
  private static final Logger LOG = Logger.getLogger(UpdateManager.class);

  private final class UpdateToDo implements Runnable {
    private final StorageEngine engine;
    private final int partNum;
    private final Queue<Throwable> exceptionQueue;
    private final int toDomainVersion;
    private final HostDomainPartition part;
    private final String domainName;
    private final int toDomainGroupVersion;
    private final Set<Integer> excludeVersions;

    public UpdateToDo(StorageEngine engine, int partNum, Queue<Throwable> exceptionQueue, int toDomainVersion, HostDomainPartition part, String domainName, int toDomainGroupVersion, Set<Integer> excludeVersions) {
      this.engine = engine;
      this.partNum = partNum;
      this.exceptionQueue = exceptionQueue;
      this.toDomainVersion = toDomainVersion;
      this.part = part;
      this.domainName = domainName;
      this.toDomainGroupVersion = toDomainGroupVersion;
      this.excludeVersions = excludeVersions;
    }

    @Override
    public void run() {
      try {
        LOG.debug(String.format("%sp%d to version %d starting (%s)", domainName, partNum, toDomainVersion, engine.toString()));
        engine.getUpdater(configurator, partNum).update(toDomainVersion, excludeVersions);
        part.setCurrentDomainGroupVersion(toDomainGroupVersion);
        part.setUpdatingToDomainGroupVersion(null);
        LOG.debug(String.format("UpdateToDo %s part %d completed.", engine.toString(), partNum));
      } catch (Throwable e) {
        LOG.fatal("Failed to complete an UpdateToDo!", e);
        exceptionQueue.add(e);
      }
    }
  }

  private final PartservConfigurator configurator;
  private final Host hostConfig;
  private final RingGroup ringGroupConfig;
  private final Ring ringConfig;

  public UpdateManager(PartservConfigurator configurator, Host hostConfig, RingGroup ringGroupConfig, Ring ringConfig) throws IOException {
    this.configurator = configurator;
    this.hostConfig = hostConfig;
    this.ringGroupConfig = ringGroupConfig;
    this.ringConfig = ringConfig;
  }

  public void update() throws IOException {
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

    DomainGroup domainGroup = ringGroupConfig.getDomainGroup();
    for (DomainGroupVersionDomainVersion dgvdv : domainGroup.getLatestVersion().getDomainVersions()) {
      Domain domain = dgvdv.getDomain();

      Set<Integer> excludeVersions = new HashSet<Integer>();
      for (DomainVersion dv : domain.getVersions()) {
        if (dv.isDefunct()) {
          excludeVersions.add(dv.getVersionNumber());
        }
      }

      StorageEngine engine = domain.getStorageEngine();

      int domainId = domainGroup.getDomainId(domain.getName());
      for (HostDomainPartition part : hostConfig.getDomainById(domainId).getPartitions()) {
        if (part.isDeletable()) {
          Deleter deleter = engine.getDeleter(configurator, part.getPartNum());
          deleter.delete();
          part.delete();
        } else if (part.getUpdatingToDomainGroupVersion() != null) {
          LOG.debug(String.format("Configuring update task for group-%s/ring-%d/domain-%s/part-%d from %d to %d",
              ringGroupConfig.getName(),
              ringConfig.getRingNumber(),
              domain.getName(),
              part.getPartNum(),
              part.getCurrentDomainGroupVersion(),
              part.getUpdatingToDomainGroupVersion()));
          executor.execute(new UpdateToDo(engine,
              part.getPartNum(),
              exceptionQueue,
              dgvdv.getVersionNumber(),
              part,
              domain.getName(),
              part.getUpdatingToDomainGroupVersion(),
              excludeVersions));
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
