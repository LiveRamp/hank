/**
 * Copyright 2011 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.liveramp.hank.partition_server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.hank.config.PartitionServerConfigurator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainAndVersion;
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

  private static final int UPDATE_EXECUTOR_TERMINATION_CHECK_TIMEOUT_VALUE = 10;
  private static final TimeUnit UPDATE_EXECUTOR_TERMINATION_CHECK_TIMEOUT_UNIT = TimeUnit.SECONDS;

  private static final Logger LOG = LoggerFactory.getLogger(UpdateManager.class);

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


  private final class PartitionUpdateTask implements Runnable, Comparable<PartitionUpdateTask> {

    private final HostDomain hostDomain;
    private final Domain domain;
    private final HostDomainPartition partition;
    private final PartitionUpdateTaskStatisticsAggregator partitionUpdateTaskStatisticsAggregator;
    private final List<Throwable> encounteredThrowables;
    private final DiskPartitionAssignment assignment;

    public PartitionUpdateTask(HostDomain hostDomain,
                               HostDomainPartition partition,
                               PartitionUpdateTaskStatisticsAggregator partitionUpdateTaskStatisticsAggregator,
                               List<Throwable> encounteredThrowables,
                               DiskPartitionAssignment assignment) {
      this.hostDomain = hostDomain;
      this.encounteredThrowables = encounteredThrowables;
      this.domain = hostDomain.getDomain();
      this.partition = partition;
      this.assignment = assignment;
      this.partitionUpdateTaskStatisticsAggregator = partitionUpdateTaskStatisticsAggregator;
      // Register itself in the aggregator
      partitionUpdateTaskStatisticsAggregator.register(this);
    }

    public Domain getDomain() {
      return domain;
    }

    public String getDataDirectory() {
      return assignment.getDisk(partition.getPartitionNumber());
    }

    @Override
    public void run() {
      PartitionUpdateTaskStatistics statistics = new PartitionUpdateTaskStatistics();
      statistics.setStartTimeMs(System.currentTimeMillis());
      try {
        // Determine target version
        DomainAndVersion targetDomainAndVersion =
            ringGroup.getDomainGroup().getDomainVersion(domain);

        // If unable to determine the version, this partition is deletable (the corresponding domain is not in the
        // target domain group version)
        if (partition.isDeletable() || targetDomainAndVersion == null) {
          deletePartition(hostDomain, partition);
        } else {
          // Determine Domain Version
          DomainVersion targetDomainVersion =
              domain.getVersion(targetDomainAndVersion.getVersionNumber());

          // Skip partitions already up-to-date
          if (partition.getCurrentDomainVersion() != null &&
              partition.getCurrentDomainVersion().equals(targetDomainVersion.getVersionNumber())) {
            LOG.info(String.format(
                "Skipping partition update of domain %s partition %d to version %d (it is already up-to-date).",
                domain.getName(), partition.getPartitionNumber(), targetDomainVersion.getVersionNumber()));
            return;
          }

          // Mark the beginning of the update by first unsetting the partition's current version number.
          // That way, if the update fails, we will have to update it again, and won't be able to serve it.
          partition.setCurrentDomainVersion(null);

          // Perform update
          StorageEngine storageEngine = domain.getStorageEngine();
          LOG.info(String.format(
              "Starting partition update of domain %s partition %d to version %d in %s.",
              domain.getName(), partition.getPartitionNumber(), targetDomainVersion.getVersionNumber(), getDataDirectory()));
          storageEngine.getUpdater(assignment, partition.getPartitionNumber()).updateTo(targetDomainVersion, statistics);

          // Record update success
          partition.setCurrentDomainVersion(targetDomainVersion.getVersionNumber());
          LOG.info(String.format(
              "Completed partition update of domain %s partition %d to version %d.",
              domain.getName(), partition.getPartitionNumber(), targetDomainVersion.getVersionNumber()));
        }
      } catch (Throwable t) {
        LOG.error(String.format("Failed to complete partition update of domain %s partition %d.",
            domain.getName(), partition.getPartitionNumber()), t);
        encounteredThrowables.add(t);
      } finally {
        statistics.setEndTimeMs(System.currentTimeMillis());
        partitionUpdateTaskStatisticsAggregator.recordPartitionUpdateTaskStatistics(this, statistics);
      }
    }

    private void deletePartition(HostDomain hostDomain,
                                 HostDomainPartition partition) throws IOException {
      LOG.info("Deleting Domain " + hostDomain.getDomain().getName() + " partition " + partition.getPartitionNumber());
      Deleter deleter = hostDomain.getDomain().getStorageEngine().getDeleter(assignment, partition.getPartitionNumber());
      deleter.delete();
      hostDomain.removePartition(partition.getPartitionNumber());
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
    private AtomicInteger threadID = new AtomicInteger(0);

    public UpdaterThreadFactory(String dataDirectory) {
      this.dataDirectory = dataDirectory;
    }

    @Override
    public Thread newThread(Runnable r) {
      return new Thread(r, "Updater Thread Pool Thread: " + dataDirectory + " #" + threadID.getAndIncrement());
    }
  }

  private static class UpdateThreadPoolExecutor extends ThreadPoolExecutor {

    private final Semaphore semaphore;

    public UpdateThreadPoolExecutor(int numThreads,
                                    ThreadFactory threadFactory,
                                    Semaphore semaphore) {
      // Essentially a fixed thread pool
      super(
          numThreads,
          numThreads,
          0L,
          TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<Runnable>(),
          threadFactory);
      this.semaphore = semaphore;
    }

    @Override
    protected void beforeExecute(Thread thread, Runnable runnable) {
      try {
        semaphore.acquire();
      } catch (InterruptedException e) {
        LOG.error("Failed to acquire update thread semaphore", e);
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void afterExecute(Runnable runnable, Throwable throwable) {
      semaphore.release();
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

  @Override
  public void update() throws IOException {
    HankTimer timer = new HankTimer();
    try {


      // Delete unknown files
      deleteUnknownFiles();
      // Perform update
      Semaphore concurrentUpdatesSemaphore = new Semaphore(configurator.getNumConcurrentUpdates());
      List<Throwable> encounteredThrowables = new ArrayList<Throwable>();
      PartitionUpdateTaskStatisticsAggregator partitionUpdateTaskStatisticsAggregator = new PartitionUpdateTaskStatisticsAggregator();
      Map<String, Queue<PartitionUpdateTask>> dataDirectoryToUpdateTasks = new HashMap<String, Queue<PartitionUpdateTask>>();
      List<PartitionUpdateTask> allUpdateTasks = buildPartitionUpdateTasks(partitionUpdateTaskStatisticsAggregator, encounteredThrowables);
      // Build and organize update tasks per data directory
      for (PartitionUpdateTask updateTask : allUpdateTasks) {
        String dataDirectory = updateTask.getDataDirectory();
        Queue<PartitionUpdateTask> updateTasks = dataDirectoryToUpdateTasks.get(dataDirectory);
        if (updateTasks == null) {
          updateTasks = new LinkedList<PartitionUpdateTask>();
          dataDirectoryToUpdateTasks.put(dataDirectory, updateTasks);
        }
        updateTasks.add(updateTask);
      }

      // Logging
      LOG.info("Number of update tasks: " + allUpdateTasks.size());
      for (Map.Entry<String, Queue<PartitionUpdateTask>> entry : dataDirectoryToUpdateTasks.entrySet()) {
        LOG.info("Number of update tasks scheduled in " + entry.getKey() + ": " + entry.getValue().size());
      }

      // Build executor services
      Map<String, ExecutorService> dataDirectoryToExecutorService = new HashMap<String, ExecutorService>();
      for (String dataDirectory : dataDirectoryToUpdateTasks.keySet()) {
        dataDirectoryToExecutorService.put(dataDirectory,
            new UpdateThreadPoolExecutor(
                configurator.getMaxConcurrentUpdatesPerDataDirectory(),
                new UpdaterThreadFactory(dataDirectory),
                concurrentUpdatesSemaphore));
      }

      LOG.info("Submitting update tasks for " + dataDirectoryToUpdateTasks.size() + " directories.");

      // Execute tasks. We execute one task for each data directory and loop around so that the tasks
      // attempt to acquire the semaphore in a reasonable order.
      boolean remaining = true;
      while (remaining) {
        remaining = false;
        for (Map.Entry<String, Queue<PartitionUpdateTask>> entry : dataDirectoryToUpdateTasks.entrySet()) {
          // Pop next task
          Queue<PartitionUpdateTask> partitionUpdateTasks = entry.getValue();
          if (!partitionUpdateTasks.isEmpty()) {
            PartitionUpdateTask partitionUpdateTask = partitionUpdateTasks.remove();
            // Execute task
            dataDirectoryToExecutorService.get(entry.getKey()).execute(partitionUpdateTask);
          }
          if (!partitionUpdateTasks.isEmpty()) {
            remaining = true;
          }
        }
      }

      LOG.info("All update tasks submitted, shutting down executor services");

      // Shutdown executors
      for (ExecutorService executorService : dataDirectoryToExecutorService.values()) {
        executorService.shutdown();
      }

      LOG.info("Waiting for executors to finish.");

      // Wait for executors to finish
      for (Map.Entry<String, ExecutorService> entry : dataDirectoryToExecutorService.entrySet()) {
        String directory = entry.getKey();
        ExecutorService executorService = entry.getValue();

        boolean keepWaiting = true;
        while (keepWaiting) {
          try {
            LOG.info("Waiting for updates to complete on data directory: " + directory);
            boolean terminated = executorService.awaitTermination(
                UPDATE_EXECUTOR_TERMINATION_CHECK_TIMEOUT_VALUE,
                UPDATE_EXECUTOR_TERMINATION_CHECK_TIMEOUT_UNIT);
            if (terminated) {
              // We finished executing all tasks
              // Otherwise, timeout elapsed and current thread was not interrupted. Keep waiting.
              LOG.info("Finished updates for directory: " + directory);
              keepWaiting = false;
            }
            // Record update ETA
            Hosts.setUpdateETA(host, partitionUpdateTaskStatisticsAggregator.computeETA());
          } catch (InterruptedException e) {
            // Received interruption (stop request).
            // Swallow the interrupted state and ask the executor to shutdown immediately. Also, keep waiting.
            LOG.info("The update manager was interrupted. Stopping the update process (stop executing new partition update tasks" +
                " and wait for those that were running to finish).");
            // Shutdown all executors
            for (ExecutorService otherExecutorService : dataDirectoryToExecutorService.values()) {
              otherExecutorService.shutdownNow();
            }
            // Record failed update exception (we need to keep waiting)
            encounteredThrowables.add(new IOException("Failed to complete update: update interruption was requested."));
          }
        }
      }

      LOG.info("All executors have finished updates");

      // Shutdown all executors
      for (ExecutorService executorService : dataDirectoryToExecutorService.values()) {
        executorService.shutdownNow();
      }

      LOG.info("Finished with " + encounteredThrowables.size() + " errors.");

      // Detect failures
      if (!encounteredThrowables.isEmpty()) {
        LOG.error(String.format("%d exceptions encountered while running partition update tasks:", encounteredThrowables.size()));
        int i = 0;
        for (Throwable t : encounteredThrowables) {
          LOG.error(String.format("Exception %d/%d:", ++i, encounteredThrowables.size()), t);
        }
        throw new IOException(String.format(
            "Failed to complete update: %d exceptions encountered while running partition update tasks.",
            encounteredThrowables.size()));
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

  private ArrayList<PartitionUpdateTask> buildPartitionUpdateTasks(
      PartitionUpdateTaskStatisticsAggregator partitionUpdateTaskStatisticsAggregator,
      List<Throwable> encounteredThrowables) throws IOException {
    ArrayList<PartitionUpdateTask> partitionUpdateTasks = new ArrayList<PartitionUpdateTask>();


    for (HostDomain hostDomain : host.getAssignedDomains()) {

      StorageEngine engine = hostDomain.getDomain().getStorageEngine();

      DiskPartitionAssignment assignments = engine.getDataDirectoryPerPartition(configurator, getPartitionNumbers(hostDomain.getPartitions()));

      for (HostDomainPartition partition : hostDomain.getPartitions()) {
        partitionUpdateTasks.add(
            new PartitionUpdateTask(
                hostDomain,
                partition,
                partitionUpdateTaskStatisticsAggregator,
                encounteredThrowables,
                assignments));
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

  private void deleteUnknownFiles() throws IOException {
    // Compute expected files
    Set<String> expectedFiles = new HashSet<String>();
    for (HostDomain hostDomain : host.getAssignedDomains()) {
      StorageEngine storageEngine = hostDomain.getDomain().getStorageEngine();

      DiskPartitionAssignment assignments = storageEngine.getDataDirectoryPerPartition(configurator, getPartitionNumbers(hostDomain.getPartitions()));

      for (HostDomainPartition hostDomainPartition : hostDomain.getPartitions()) {
        Integer versionNumber = hostDomainPartition.getCurrentDomainVersion();
        if (versionNumber != null) {
          for (String filePath : storageEngine.getFiles(assignments, versionNumber, hostDomainPartition.getPartitionNumber())) {
            File file = new File(filePath);
            // Add the file itself
            expectedFiles.add(file.getCanonicalPath());
            // Add all parent directories
            File parent = file.getParentFile();
            while (parent != null) {
              expectedFiles.add(parent.getCanonicalPath());
              parent = parent.getParentFile();
            }
          }
        }
      }
    }
    // Delete unknown files
    for (String dataDirectoryPath : configurator.getDataDirectories()) {
      LOG.info("Deleting unknown files in " + dataDirectoryPath);
      File dataDirectory = new File(dataDirectoryPath);
      if (dataDirectory.exists()) {
        for (File file : FileUtils.listFiles(dataDirectory, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)) {
          if (!expectedFiles.contains(file.getCanonicalPath())) {
            LOG.info("Deleting unknown file: " + file.getCanonicalPath());
            FileUtils.deleteQuietly(file);
          }
        }
      }
    }
  }

  private static Set<Integer> getPartitionNumbers(Collection<HostDomainPartition> partition) {
    Set<Integer> partitionNumbers = Sets.newHashSet();
    for (HostDomainPartition hostDomainPartition : partition) {
      partitionNumbers.add(hostDomainPartition.getPartitionNumber());
    }
    return partitionNumbers;
  }
}
