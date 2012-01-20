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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.*;

/**
 * Manages the domain update process.
 */
class UpdateManager implements IUpdateManager {

  private static final int UPDATE_EXECUTOR_TERMINATION_CHECK_TIMEOUT_VALUE = 10;
  private static final TimeUnit UPDATE_EXECUTOR_TERMINATION_CHECK_TIMEOUT_UNIT = TimeUnit.SECONDS;
  private static final Logger LOG = Logger.getLogger(UpdateManager.class);

  private final class PartitionUpdateTask implements Runnable {

    private final HostDomain hostDomain;
    private final Domain domain;
    private final HostDomainPartition partition;
    private final Queue<Throwable> exceptionQueue;

    public PartitionUpdateTask(HostDomain hostDomain,
                               HostDomainPartition partition,
                               Queue<Throwable> exceptionQueue) {
      this.hostDomain = hostDomain;
      this.domain = hostDomain.getDomain();
      this.partition = partition;
      this.exceptionQueue = exceptionQueue;
    }

    @Override
    public void run() {
      try {
        // Determine target version
        DomainGroupVersion targetDomainGroupVersion = ringGroup.getTargetVersion();
        DomainGroupVersionDomainVersion targetDomainGroupVersionDomainVersion =
            targetDomainGroupVersion.getDomainVersion(domain);

        // If unable to determine the version, this partition is deletable (the corresponding domain is not in the
        // target domain group version)
        if (partition.isDeletable() || targetDomainGroupVersionDomainVersion == null) {
          deletePartition(hostDomain, partition);
        } else {
          // Determine Domain Version
          DomainVersion targetDomainVersion =
              domain.getVersionByNumber(targetDomainGroupVersionDomainVersion.getVersion());

          // Skip partitions already up-to-date
          if (partition.getCurrentDomainGroupVersion() != null &&
              partition.getCurrentDomainGroupVersion().equals(targetDomainGroupVersion.getVersionNumber())) {
            LOG.info(String.format(
                "Skipping partition update of domain %s partition %d to version %d (it is already up-to-date).",
                domain.getName(), partition.getPartitionNumber(), targetDomainVersion.getVersionNumber()));
            return;
          }

          // Perform update
          StorageEngine storageEngine = domain.getStorageEngine();
          LOG.info(String.format(
              "Starting partition update of domain %s partition %d to version %d.",
              domain.getName(), partition.getPartitionNumber(), targetDomainVersion.getVersionNumber()));
          storageEngine.getUpdater(configurator, partition.getPartitionNumber()).updateTo(targetDomainVersion);

          // Record update suceess
          partition.setCurrentDomainGroupVersion(targetDomainGroupVersion.getVersionNumber());
          LOG.info(String.format(
              "Completed partition update of domain %s partition %d to version %d.",
              domain.getName(), partition.getPartitionNumber(), targetDomainVersion.getVersionNumber()));
        }
      } catch (Throwable e) {
        LOG.fatal(String.format("Failed to complete partition update of domain %s partition %d.",
            domain.getName(), partition.getPartitionNumber()), e);
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

    // Perform update
    ThreadFactory factory = new ThreadFactory() {
      private int threadID = 0;

      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "Updater Thread Pool Thread #" + ++threadID);
      }
    };

    ExecutorService executor = Executors.newFixedThreadPool(configurator.getNumConcurrentUpdates(), factory);

    Queue<Throwable> exceptionQueue = new LinkedBlockingQueue<Throwable>();

    executePartitionUpdateTasks(executor, exceptionQueue);

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

    // Garbage collect useless host domains
    garbageCollectHostDomains(host);
  }

  private void executePartitionUpdateTasks(ExecutorService executor,
                                           Queue<Throwable> exceptionQueue) throws IOException {
    ArrayList<PartitionUpdateTask> partitionUpdateTasks = new ArrayList<PartitionUpdateTask>();

    for (HostDomain hostDomain : host.getAssignedDomains()) {
      for (HostDomainPartition partition : hostDomain.getPartitions()) {
        partitionUpdateTasks.add(new PartitionUpdateTask(hostDomain, partition, exceptionQueue));
      }
    }

    // Randomize task ordering so that it updates all domains more or less concurrently
    Collections.shuffle(partitionUpdateTasks);

    // Execute tasks
    for (PartitionUpdateTask updateTask : partitionUpdateTasks) {
      executor.execute(updateTask);
    }
  }

  private void garbageCollectHostDomains(Host host) throws IOException {
    // Delete deletable domains and partitions
    for (HostDomain hostDomain : host.getAssignedDomains()) {
      // Host domain does not contain anymore partitions. Delete it
      if (hostDomain.getPartitions().size() == 0) {
        LOG.info("Garbage collecting Host Domain " + hostDomain + " as it is not used anymore.");
        host.removeDomain(hostDomain.getDomain());
      }
    }
  }

  private void deletePartition(HostDomain hostDomain,
                               HostDomainPartition partition) throws IOException {
    LOG.info("Deleting Domain " + hostDomain.getDomain().getName() + " partition " + partition.getPartitionNumber());
    Deleter deleter = hostDomain.getDomain().getStorageEngine().getDeleter(configurator, partition.getPartitionNumber());
    deleter.delete();
    hostDomain.removePartition(partition.getPartitionNumber());
  }
}
