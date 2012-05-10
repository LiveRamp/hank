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
import java.util.*;
import java.util.concurrent.*;

/**
 * Manages the domain update process.
 */
public class UpdateManager implements IUpdateManager {

  private static final int UPDATE_EXECUTOR_TERMINATION_CHECK_TIMEOUT_VALUE = 10;
  private static final TimeUnit UPDATE_EXECUTOR_TERMINATION_CHECK_TIMEOUT_UNIT = TimeUnit.SECONDS;
  private static final String UPDATE_ETA_STATISTIC_KEY = "update_eta";
  private static final Logger LOG = Logger.getLogger(UpdateManager.class);

  private final class PartitionUpdateTaskStatistics {

    private final long startTimeMs;
    private final long endTimeMs;

    private PartitionUpdateTaskStatistics(long startTimeMs, long endTimeMs) {
      this.startTimeMs = startTimeMs;
      this.endTimeMs = endTimeMs;
    }

    public long getStartTimeMs() {
      return startTimeMs;
    }

    public long getEndTimeMs() {
      return endTimeMs;
    }
  }

  private final class PartitionUpdateTaskStatisticsAggregator {

    static private final int NUM_PARTITIONS_USED_FOR_PROJECTION_RATIO = 10;
    static private final int MIN_NUM_PARTITIONS_USED_FOR_PROJECTION = 10;

    private final Map<Domain, List<PartitionUpdateTaskStatistics>> domainToPartitionUpdateTaskStatistics
        = new HashMap<Domain, List<PartitionUpdateTaskStatistics>>();
    private final Map<Domain, Integer> domainToNumPartitionUpdateTasks = new HashMap<Domain, Integer>();

    public synchronized void recordPartitionUpdateTaskStatistics(PartitionUpdateTask partitionUpdateTask,
                                                                 PartitionUpdateTaskStatistics partitionUpdateTaskStatistics) {
      List<PartitionUpdateTaskStatistics> partitionUpdateTaskStatisticsList
          = domainToPartitionUpdateTaskStatistics.get(partitionUpdateTask.getDomain());
      partitionUpdateTaskStatisticsList.add(partitionUpdateTaskStatistics);
    }

    public synchronized void register(PartitionUpdateTask partitionUpdateTask) {
      Domain domain = partitionUpdateTask.getDomain();
      // Initialize maps
      if (!domainToPartitionUpdateTaskStatistics.containsKey(domain)) {
        domainToPartitionUpdateTaskStatistics.put(domain, new ArrayList<PartitionUpdateTaskStatistics>());
      }
      Integer numPartitionUpdateTasks = domainToNumPartitionUpdateTasks.get(domain);
      if (numPartitionUpdateTasks == null) {
        domainToNumPartitionUpdateTasks.put(domain, 1);
      } else {
        domainToNumPartitionUpdateTasks.put(domain, numPartitionUpdateTasks + 1);
      }
    }

    /**
     * @return ETA in seconds, a negative number if no ETA could be computed
     */
    public synchronized long computeETA() {
      long maxDomainETA = -1;
      // For each domain, compute the number of updated partitions per second
      for (Map.Entry<Domain, List<PartitionUpdateTaskStatistics>> entry : domainToPartitionUpdateTaskStatistics.entrySet()) {
        Domain domain = entry.getKey();
        List<PartitionUpdateTaskStatistics> partitionUpdateTaskStatisticsList = entry.getValue();
        // Only consider at a fixed number of partitions in the past
        int numPartitionUpdateTasksForDomain = domainToNumPartitionUpdateTasks.get(domain);
        int numPartitionsToConsider = numPartitionUpdateTasksForDomain / NUM_PARTITIONS_USED_FOR_PROJECTION_RATIO;
        if (numPartitionsToConsider < MIN_NUM_PARTITIONS_USED_FOR_PROJECTION) {
          numPartitionsToConsider = MIN_NUM_PARTITIONS_USED_FOR_PROJECTION;
        }
        numPartitionsToConsider = Math.min(numPartitionsToConsider, numPartitionUpdateTasksForDomain);
        // Consider statistics
        int firstIndex = Math.max(0, partitionUpdateTaskStatisticsList.size() - numPartitionsToConsider);
        long minStartTimeMs = -1;
        long maxEndTimeMs = -1;
        // Compute time window for the chosen subset of partition update statistics
        for (int i = firstIndex; i < partitionUpdateTaskStatisticsList.size(); ++i) {
          long startTimeMs = partitionUpdateTaskStatisticsList.get(i).getStartTimeMs();
          long endTimeMs = partitionUpdateTaskStatisticsList.get(i).getEndTimeMs();
          if (minStartTimeMs < 0 || startTimeMs < minStartTimeMs) {
            minStartTimeMs = startTimeMs;
          }
          if (maxEndTimeMs < 0 || endTimeMs > maxEndTimeMs) {
            maxEndTimeMs = endTimeMs;
          }
        }
        // Compute window statistics
        long windowDurationMS = maxEndTimeMs - minStartTimeMs;
        long numPartitionUpdateTasksFinishedInWindow = Math.min(partitionUpdateTaskStatisticsList.size(), numPartitionsToConsider);
        if (windowDurationMS == 0 || numPartitionUpdateTasksFinishedInWindow == 0) {
          return -1;
        }
        // Compute time taken by partition updates of this domain
        double numSecondsPerPartitionUpdateTask = ((double) windowDurationMS / 1000.0d) / (double) numPartitionUpdateTasksFinishedInWindow;
        // Compute ETA in seconds for this domain
        long numRemainingPartitionUpdateTasksForDomain = numPartitionUpdateTasksForDomain - partitionUpdateTaskStatisticsList.size();
        long domainETA = Math.round(numRemainingPartitionUpdateTasksForDomain * numSecondsPerPartitionUpdateTask);
        if (domainETA > maxDomainETA) {
          maxDomainETA = domainETA;
        }
      }
      return maxDomainETA;
    }
  }

  private final class PartitionUpdateTask implements Runnable, Comparable<PartitionUpdateTask> {

    private final HostDomain hostDomain;
    private final Domain domain;
    private final HostDomainPartition partition;
    private final PartitionUpdateTaskStatisticsAggregator partitionUpdateTaskStatisticsAggregator;
    private final Queue<Throwable> exceptionQueue;

    public PartitionUpdateTask(HostDomain hostDomain,
                               HostDomainPartition partition,
                               PartitionUpdateTaskStatisticsAggregator partitionUpdateTaskStatisticsAggregator,
                               Queue<Throwable> exceptionQueue) {
      this.hostDomain = hostDomain;
      this.domain = hostDomain.getDomain();
      this.partition = partition;
      this.partitionUpdateTaskStatisticsAggregator = partitionUpdateTaskStatisticsAggregator;
      this.exceptionQueue = exceptionQueue;
      // Register itself in the aggregator
      partitionUpdateTaskStatisticsAggregator.register(this);
    }

    public Domain getDomain() {
      return domain;
    }

    @Override
    public void run() {
      long startTimeMs = System.currentTimeMillis();
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
              domain.getVersion(targetDomainGroupVersionDomainVersion.getVersion());

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
      } finally {
        long endTimeMs = System.currentTimeMillis();
        partitionUpdateTaskStatisticsAggregator.recordPartitionUpdateTaskStatistics(this,
            new PartitionUpdateTaskStatistics(startTimeMs, endTimeMs));
      }
    }

    @Override
    public int compareTo(PartitionUpdateTask other) {
      if (partition.getPartitionNumber() < other.partition.getPartitionNumber()) {
        return -1;
      } else if (partition.getPartitionNumber() > other.partition.getPartitionNumber()) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  private final PartitionServerConfigurator configurator;
  private final Host host;
  private final RingGroup ringGroup;

  public UpdateManager(PartitionServerConfigurator configurator, Host host, RingGroup ringGroup) throws IOException {
    this.configurator = configurator;
    this.host = host;
    this.ringGroup = ringGroup;
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
    PartitionUpdateTaskStatisticsAggregator partitionUpdateTaskStatisticsAggregator
        = new PartitionUpdateTaskStatisticsAggregator();
    Queue<Throwable> exceptionQueue = new LinkedBlockingQueue<Throwable>();

    // Execute all tasks and wait for them to finish
    executePartitionUpdateTasks(executor, partitionUpdateTaskStatisticsAggregator, exceptionQueue);
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
        // Record update ETA
        setUpdateETA(host, partitionUpdateTaskStatisticsAggregator.computeETA());
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
                                           PartitionUpdateTaskStatisticsAggregator partitionUpdateTaskStatisticsAggregator,
                                           Queue<Throwable> exceptionQueue) throws IOException {
    ArrayList<PartitionUpdateTask> partitionUpdateTasks = new ArrayList<PartitionUpdateTask>();

    for (HostDomain hostDomain : host.getAssignedDomains()) {
      for (HostDomainPartition partition : hostDomain.getPartitions()) {
        partitionUpdateTasks.add(new PartitionUpdateTask(hostDomain, partition,
            partitionUpdateTaskStatisticsAggregator, exceptionQueue));
      }
    }

    // Sort update tasks per partition id, so that we update domains concurrently but in order of partition number
    Collections.sort(partitionUpdateTasks);

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

  public static void setUpdateETA(Host host, long updateETA) throws IOException {
    host.setEphemeralStatistic(UPDATE_ETA_STATISTIC_KEY, Long.toString(updateETA));
  }

  public static long getUpdateETA(Host host) {
    try {
      if (host.getState() != HostState.UPDATING) {
        return -1;
      }
      String etaString = host.getStatistic(UPDATE_ETA_STATISTIC_KEY);
      if (etaString != null) {
        return Long.parseLong(etaString);
      } else {
        return -1;
      }
    } catch (IOException e) {
      return -1;
    }
  }
}
