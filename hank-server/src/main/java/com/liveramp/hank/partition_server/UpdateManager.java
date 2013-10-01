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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.liveramp.hank.config.PartitionServerConfigurator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainGroupDomainVersion;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostDomainPartition;
import com.liveramp.hank.coordinator.Hosts;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.storage.Deleter;
import com.liveramp.hank.storage.StorageEngine;
import com.liveramp.hank.util.DurationAggregator;
import com.liveramp.hank.util.FormatUtils;
import com.liveramp.hank.util.HankTimer;

/**
 * Manages the domain update process.
 */
public class UpdateManager implements IUpdateManager {

  private static final Logger LOG = Logger.getLogger(UpdateManager.class);

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
        double numSecondsPerPartitionUpdateTask = ((double)windowDurationMS / 1000.0d) / (double)numPartitionUpdateTasksFinishedInWindow;
        // Compute ETA in seconds for this domain
        long numRemainingPartitionUpdateTasksForDomain = numPartitionUpdateTasksForDomain - partitionUpdateTaskStatisticsList.size();
        long domainETA = Math.round(numRemainingPartitionUpdateTasksForDomain * numSecondsPerPartitionUpdateTask);
        if (domainETA > maxDomainETA) {
          maxDomainETA = domainETA;
        }
      }
      return maxDomainETA;
    }

    public synchronized void logStats() {
      Map<String, DurationAggregator> hankTimerDurationAggregators = new TreeMap<String, DurationAggregator>();
      for (Map.Entry<Domain, List<PartitionUpdateTaskStatistics>> entry1 : domainToPartitionUpdateTaskStatistics.entrySet()) {
        Domain domain = entry1.getKey();
        List<PartitionUpdateTaskStatistics> partitionUpdateTaskStatisticsList = entry1.getValue();
        for (PartitionUpdateTaskStatistics partitionUpdateTaskStatistics : partitionUpdateTaskStatisticsList) {
          for (Map.Entry<String, Long> entry2 : partitionUpdateTaskStatistics.getDurationsMs().entrySet()) {
            String name = domain.getName() + " - " + entry2.getKey();
            Long duration = entry2.getValue();
            DurationAggregator aggregator = hankTimerDurationAggregators.get(name);
            if (aggregator == null) {
              aggregator = new DurationAggregator(name);
              hankTimerDurationAggregators.put(name, aggregator);
            }
            aggregator.add(duration);
          }
        }
      }
      for (DurationAggregator aggregator : hankTimerDurationAggregators.values()) {
        aggregator.logStats();
      }
    }
  }

  private final class PartitionUpdateTask implements Callable<Boolean>, Comparable<PartitionUpdateTask> {

    private final HostDomain hostDomain;
    private final Domain domain;
    private final HostDomainPartition partition;
    private final String dataDirectory;
    private final PartitionUpdateTaskStatisticsAggregator partitionUpdateTaskStatisticsAggregator;

    public PartitionUpdateTask(HostDomain hostDomain,
                               HostDomainPartition partition,
                               PartitionUpdateTaskStatisticsAggregator partitionUpdateTaskStatisticsAggregator) {
      this.hostDomain = hostDomain;
      this.domain = hostDomain.getDomain();
      this.partition = partition;
      this.dataDirectory = domain.getStorageEngine().getDataDirectory(configurator, partition.getPartitionNumber());
      this.partitionUpdateTaskStatisticsAggregator = partitionUpdateTaskStatisticsAggregator;
      // Register itself in the aggregator
      partitionUpdateTaskStatisticsAggregator.register(this);
    }

    public Domain getDomain() {
      return domain;
    }

    public String getDataDirectory() {
      return dataDirectory;
    }

    @Override
    public Boolean call() throws Exception {
      PartitionUpdateTaskStatistics statistics = new PartitionUpdateTaskStatistics();
      statistics.setStartTimeMs(System.currentTimeMillis());
      try {
        // Determine target version
        DomainGroupDomainVersion targetDomainGroupDomainVersion =
            ringGroup.getDomainGroup().getDomainVersion(domain);

        // If unable to determine the version, this partition is deletable (the corresponding domain is not in the
        // target domain group version)
        if (partition.isDeletable() || targetDomainGroupDomainVersion == null) {
          deletePartition(hostDomain, partition);
        } else {
          // Determine Domain Version
          DomainVersion targetDomainVersion =
              domain.getVersion(targetDomainGroupDomainVersion.getVersionNumber());

          // Skip partitions already up-to-date
          if (partition.getCurrentDomainVersion() != null &&
              partition.getCurrentDomainVersion().equals(targetDomainVersion.getVersionNumber())) {
            LOG.info(String.format(
                "Skipping partition update of domain %s partition %d to version %d (it is already up-to-date).",
                domain.getName(), partition.getPartitionNumber(), targetDomainVersion.getVersionNumber()));
            return true;
          }

          // Mark the beginning of the update by first unsetting the partition's current version number.
          // That way, if the update fails, we will have to update it again, and won't be able to serve it.
          partition.setCurrentDomainVersion(null);

          // Perform update
          StorageEngine storageEngine = domain.getStorageEngine();
          LOG.info(String.format(
              "Starting partition update of domain %s partition %d to version %d in %s.",
              domain.getName(), partition.getPartitionNumber(), targetDomainVersion.getVersionNumber(), getDataDirectory()));
          storageEngine.getUpdater(configurator, partition.getPartitionNumber()).updateTo(targetDomainVersion, statistics);

          // Record update success
          partition.setCurrentDomainVersion(targetDomainVersion.getVersionNumber());
          LOG.info(String.format(
              "Completed partition update of domain %s partition %d to version %d.",
              domain.getName(), partition.getPartitionNumber(), targetDomainVersion.getVersionNumber()));
        }
      } catch (Throwable t) {
        LOG.fatal(String.format("Failed to complete partition update of domain %s partition %d.",
            domain.getName(), partition.getPartitionNumber()), t);
        throw new RuntimeException(t);
      } finally {
        statistics.setEndTimeMs(System.currentTimeMillis());
        partitionUpdateTaskStatisticsAggregator.recordPartitionUpdateTaskStatistics(this, statistics);
      }
      return true;
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

  private static class UpdaterThreadFactory implements ThreadFactory {

    private final String dataDirectory;
    private int threadID = 0;

    public UpdaterThreadFactory(String dataDirectory) {
      this.dataDirectory = dataDirectory;
    }

    @Override
    public Thread newThread(Runnable r) {
      return new Thread(r, "Updater Thread Pool Thread: " + dataDirectory + " #" + ++threadID);
    }
  }

  private static class UpdateThreadPoolExecutor extends ThreadPoolExecutor {

    private final Host host;
    private final PartitionUpdateTaskStatisticsAggregator partitionUpdateTaskStatisticsAggregator;

    public UpdateThreadPoolExecutor(int numThreads,
                                    ThreadFactory threadFactory,
                                    Host host,
                                    PartitionUpdateTaskStatisticsAggregator partitionUpdateTaskStatisticsAggregator) {
      // Essentially a fixed thread pool
      super(
          numThreads,
          numThreads,
          0L,
          TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<Runnable>(),
          threadFactory);
      this.host = host;
      this.partitionUpdateTaskStatisticsAggregator = partitionUpdateTaskStatisticsAggregator;
    }

    @Override
    protected void afterExecute(Runnable runnable, Throwable throwable) {
      // Record update ETA after execute
      try {
        Hosts.setUpdateETA(host, partitionUpdateTaskStatisticsAggregator.computeETA());
      } catch (IOException e) {
        LOG.error("Failed to set ETA", e);
        throw new RuntimeException(e);
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
  @Override
  public void update() throws IOException {
    HankTimer timer = new HankTimer();
    try {
      // Perform update
      List<Throwable> throwablesEncountered = new ArrayList<Throwable>();
      PartitionUpdateTaskStatisticsAggregator partitionUpdateTaskStatisticsAggregator = new PartitionUpdateTaskStatisticsAggregator();
      Map<String, List<PartitionUpdateTask>> dataDirectoryToUpdateTasks = new HashMap<String, List<PartitionUpdateTask>>();
      List<PartitionUpdateTask> allUpdateTasks = buildPartitionUpdateTasks(partitionUpdateTaskStatisticsAggregator);
      // Build and organize update tasks per data directory
      for (PartitionUpdateTask updateTask : allUpdateTasks) {
        String dataDirectory = updateTask.getDataDirectory();
        List<PartitionUpdateTask> updateTasks = dataDirectoryToUpdateTasks.get(dataDirectory);
        if (updateTasks == null) {
          updateTasks = new ArrayList<PartitionUpdateTask>();
          dataDirectoryToUpdateTasks.put(dataDirectory, updateTasks);
        }
        updateTasks.add(updateTask);
      }

      // Logging
      LOG.info("Number of update tasks: " + allUpdateTasks.size());
      for (Map.Entry<String, List<PartitionUpdateTask>> entry : dataDirectoryToUpdateTasks.entrySet()) {
        LOG.info("Number of update tasks scheduled in " + entry.getKey() + ": " + entry.getValue().size());
      }

      // Build executor services and invoke tasks
      List<ExecutorService> executorServices = new ArrayList<ExecutorService>();
      List<Future<Boolean>> futurePartitionUpdateTasks = new ArrayList<Future<Boolean>>();
      for (Map.Entry<String, List<PartitionUpdateTask>> entry : dataDirectoryToUpdateTasks.entrySet()) {
        ExecutorService executorService = new UpdateThreadPoolExecutor(
            configurator.getNumConcurrentUpdates(),
            new UpdaterThreadFactory(entry.getKey()),
            host,
            partitionUpdateTaskStatisticsAggregator);
        executorServices.add(executorService);
        try {
          futurePartitionUpdateTasks.addAll(executorService.invokeAll(entry.getValue()));
        } catch (InterruptedException e) {
          throwablesEncountered.add(e);
        }
      }

      // Wait for tasks to finish
      try {
        for (Future<Boolean> futurePartitionUpdateTask : futurePartitionUpdateTasks) {
          try {
            futurePartitionUpdateTask.get();
          } catch (ExecutionException e) {
            // Record task execution exception
            throwablesEncountered.add(e);
          }
        }
      } catch (InterruptedException e) {
        throwablesEncountered.add(e);
      }

      // Shutdown all executors
      for (ExecutorService executorService : executorServices) {
        executorService.shutdownNow();
      }

      // Detect failures
      if (!throwablesEncountered.isEmpty()) {
        LOG.fatal(String.format("%d exceptions encountered while running partition update tasks:", throwablesEncountered.size()));
        int i = 0;
        for (Throwable t : throwablesEncountered) {
          LOG.fatal(String.format("Exception %d/%d:", ++i, throwablesEncountered.size()), t);
        }
        throw new IOException(String.format(
            "Failed to complete update: %d exceptions encountered while running partition update tasks.",
            throwablesEncountered.size()));
      }

      // Garbage collect useless host domains
      garbageCollectHostDomains(host);

      // Log statistics
      partitionUpdateTaskStatisticsAggregator.logStats();

    } catch (IOException e) {
      LOG.info("Update failed and took " + FormatUtils.formatSecondsDuration(timer.getDurationMs() / 1000));
      throw e;
    }
    LOG.info("Update succeeded and took " + FormatUtils.formatSecondsDuration(timer.getDurationMs() / 1000));
  }

  private ArrayList<PartitionUpdateTask> buildPartitionUpdateTasks(PartitionUpdateTaskStatisticsAggregator partitionUpdateTaskStatisticsAggregator) throws IOException {
    ArrayList<PartitionUpdateTask> partitionUpdateTasks = new ArrayList<PartitionUpdateTask>();

    for (HostDomain hostDomain : host.getAssignedDomains()) {
      for (HostDomainPartition partition : hostDomain.getPartitions()) {
        partitionUpdateTasks.add(
            new PartitionUpdateTask(
                hostDomain,
                partition,
                partitionUpdateTaskStatisticsAggregator));
      }
    }

    // Sort update tasks per partition id, so that we update domains concurrently but in order of partition number
    Collections.sort(partitionUpdateTasks);

    return partitionUpdateTasks;
  }

  private void garbageCollectHostDomains(Host host) throws IOException {
    // Delete deletable domains and partitions
    for (HostDomain hostDomain : host.getAssignedDomains()) {
      // Host domain does not contain anymore partitions. Delete it
      LOG.info("Host Domain " + hostDomain + " is assigned " + hostDomain.getPartitions().size() + " partitions.");
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
