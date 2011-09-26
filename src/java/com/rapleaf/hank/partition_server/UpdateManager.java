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
  private static final int UPDATE_EXECUTOR_TERMINATION_CHECK_TIMEOUT_VALUE = 10;
  private static final TimeUnit UPDATE_EXECUTOR_TERMINATION_CHECK_TIMEOUT_UNIT = TimeUnit.SECONDS;
  private static final long UPDATE_EXECUTOR_KEEPALIVE_TIME_VALUE = 1;
  private static final TimeUnit UPDATE_EXECUTOR_KEEPALIVE_TIME_UNIT = TimeUnit.SECONDS;
  private static final Logger LOG = Logger.getLogger(UpdateManager.class);

  private final class PartitionUpdateTask implements Runnable {
    private final StorageEngine engine;
    private final int partitionNumber;
    private final Queue<Throwable> exceptionQueue;
    private final int toDomainVersion;
    private final HostDomainPartition partition;
    private final String domainName;
    private final int toDomainGroupVersion;
    private final Set<Integer> excludeVersions;

    public PartitionUpdateTask(StorageEngine engine,
                               int partitionNumber,
                               Queue<Throwable> exceptionQueue,
                               int toDomainVersion,
                               HostDomainPartition partition,
                               String domainName,
                               int toDomainGroupVersion,
                               Set<Integer> excludeVersions) {
      this.engine = engine;
      this.partitionNumber = partitionNumber;
      this.exceptionQueue = exceptionQueue;
      this.toDomainVersion = toDomainVersion;
      this.partition = partition;
      this.domainName = domainName;
      this.toDomainGroupVersion = toDomainGroupVersion;
      this.excludeVersions = excludeVersions;
    }

    @Override
    public void run() {
      try {
        LOG.info(String.format("%s part %d to version %d starting (%s)", domainName, partitionNumber, toDomainVersion, engine.toString()));
        engine.getUpdater(configurator, partitionNumber).update(toDomainVersion, excludeVersions);
        partition.setCurrentDomainGroupVersion(toDomainGroupVersion);
        partition.setUpdatingToDomainGroupVersion(null);
        LOG.info(String.format("PartitionUpdate %s part %d completed.", engine.toString(), partitionNumber));
      } catch (Throwable e) {
        LOG.fatal("Failed to complete a PartitionUpdateTask!", e);
        exceptionQueue.add(e);
      }
    }
  }

  private final PartitionServerConfigurator configurator;
  private final Host host;
  private final RingGroup ringGroup;
  private final Ring ring;

  public UpdateManager(PartitionServerConfigurator configurator, Host host, RingGroup ringGroup, Ring ring) throws IOException {
    this.configurator = configurator;
    this.host = host;
    this.ringGroup = ringGroup;
    this.ring = ring;
  }

  // When an Exception is thrown, the update has failed.
  public void update() throws IOException {
    ThreadFactory factory = new ThreadFactory() {
      private int threadID = 0;

      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "Updater Thread Pool Thread #" + ++threadID);
      }
    };

    ExecutorService executor = new ThreadPoolExecutor(
        configurator.getNumConcurrentUpdates(),
        configurator.getNumConcurrentUpdates(),
        UPDATE_EXECUTOR_KEEPALIVE_TIME_VALUE,
        UPDATE_EXECUTOR_KEEPALIVE_TIME_UNIT,
        new LinkedBlockingQueue<Runnable>(),
        factory);
    Queue<Throwable> exceptionQueue = new LinkedBlockingQueue<Throwable>();

    DomainGroup domainGroup = ringGroup.getDomainGroup();
    for (DomainGroupVersionDomainVersion dgvdv : domainGroup.getVersionByNumber(ringGroup.getUpdatingToVersion()).getDomainVersions()) {
      Domain domain = dgvdv.getDomain();

      Set<Integer> excludeVersions = new HashSet<Integer>();
      for (DomainVersion dv : domain.getVersions()) {
        if (dv.isDefunct()) {
          excludeVersions.add(dv.getVersionNumber());
        }
      }

      StorageEngine engine = domain.getStorageEngine();
      HostDomain hd = host.getHostDomain(domain);
      if (hd == null) {
        LOG.error(String.format("Host %s does not seem to be assigned Domain %s (Could not get corresponding HostDomain). Will not update.", host, domain));
      } else {
        for (HostDomainPartition part : hd.getPartitions()) {
          if (part.isDeletable()) {
            Deleter deleter = engine.getDeleter(configurator, part.getPartNum());
            deleter.delete();
            part.delete();
          } else if (part.getUpdatingToDomainGroupVersion() != null) {
            LOG.debug(String.format("Configuring update task for group-%s/ring-%d/domain-%s/part-%d from %d to %d",
                ringGroup.getName(),
                ring.getRingNumber(),
                domain.getName(),
                part.getPartNum(),
                part.getCurrentDomainGroupVersion(),
                part.getUpdatingToDomainGroupVersion()));
            executor.execute(new PartitionUpdateTask(engine,
                part.getPartNum(),
                exceptionQueue,
                dgvdv.getVersionOrAction().getVersion(),
                part,
                domain.getName(),
                part.getUpdatingToDomainGroupVersion(),
                excludeVersions));
          }
        }
      }
    }

    // Execute all tasks and wait for them to finish
    IOException failedUpdateException = null;
    boolean keepWaiting = true;
    executor.shutdown();
    while (keepWaiting) {
      LOG.debug("Waiting for update executor to complete...");
      try {
        boolean terminated = executor.awaitTermination(UPDATE_EXECUTOR_TERMINATION_CHECK_TIMEOUT_VALUE,
            UPDATE_EXECUTOR_TERMINATION_CHECK_TIMEOUT_UNIT);
        if (terminated) {
          // We finished executing all tasks
          keepWaiting = false;
        } else {
          // Timeout elapsed and current thread was not interrupted. Keep waiting.
        }
      } catch (InterruptedException e) {
        // Received interruption (stop request).
        // Swallow the interrupted state and ask the executor to shutdown immediately. Also, keep waiting.
        LOG.info("The update manager was interrupted. Stopping the update process (stop executing new partition update tasks" +
            " and wait for those that were running to finish).");
        executor.shutdownNow();
        // Record failed update exception (we need to keep waiting)
        failedUpdateException = new IOException("Failed to complete update: update interruption was requested.");
      }
    }

    // Detect failed tasks
    IOException failedTasksException = null;
    if (!exceptionQueue.isEmpty()) {
      LOG.fatal(String.format("%d exceptions encountered while running partition update tasks:", exceptionQueue.size()));
      int i = 0;
      for (Throwable t : exceptionQueue) {
        LOG.fatal(String.format("Exception %d/%d:", ++i, exceptionQueue.size()), t);
      }
      failedTasksException = new IOException(String.format(
          "Failed to complete update: %d exceptions encountered while running partition update tasks.",
          exceptionQueue.size()));
    }

    // First detect failed update
    if (failedUpdateException != null) {
      throw failedUpdateException;
    }

    // Then detect failed tasks
    if (failedTasksException != null) {
      throw failedTasksException;
    }
  }
}
