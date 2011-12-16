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

import com.rapleaf.hank.coordinator.HostDomain;
import com.rapleaf.hank.generated.HankException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.ReaderResult;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class that manages accessing data on behalf of a particular Domain.
 */
public class DomainAccessor {

  private static final HankResponse WRONG_HOST = HankResponse.xception(HankException.wrong_host(true));

  private static final Logger LOG = Logger.getLogger(DomainAccessor.class);
  private final HostDomain hostDomain;
  private final PartitionAccessor[] partitionAccessors;
  private final Partitioner partitioner;
  private final int updateStatisticsThreadSleepTimeMS;
  private final UpdateStatisticsRunnable updateStatisticsRunnable;
  private final Thread updateStatisticsThread;

  private static final int UPDATE_STATISTICS_THREAD_SLEEP_TIME_MS_DEFAULT = 30000;
  private static final String RUNTIME_STATISTICS_KEY = "runtime_statistics";

  public DomainAccessor(HostDomain hostDomain,
                        PartitionAccessor[] partitionAccessors,
                        Partitioner partitioner) throws IOException {
    this(hostDomain, partitionAccessors, partitioner, UPDATE_STATISTICS_THREAD_SLEEP_TIME_MS_DEFAULT);
  }

  DomainAccessor(HostDomain hostDomain, PartitionAccessor[] partitionAccessors,
                 Partitioner partitioner, int updateStatisticsThreadSleepTimeMS) throws IOException {
    this.hostDomain = hostDomain;
    this.partitionAccessors = partitionAccessors;
    this.partitioner = partitioner;
    this.updateStatisticsThreadSleepTimeMS = updateStatisticsThreadSleepTimeMS;

    updateStatisticsRunnable = new UpdateStatisticsRunnable();
    updateStatisticsThread = new Thread(updateStatisticsRunnable, "Update Statistics");
    updateStatisticsThread.start();
  }

  public HankResponse get(ByteBuffer key, ReaderResult result) throws IOException {
    LOG.trace("Domain GET");
    int partition = partitioner.partition(key, partitionAccessors.length);
    PartitionAccessor partitionAccessor = partitionAccessors[partition];
    if (partitionAccessor == null) {
      return WRONG_HOST;
    }
    return partitionAccessor.get(key, result);
  }

  /**
   * This thread periodically updates the counters on the HostDomainPartition
   * with the values in the cached counters
   */
  private class UpdateStatisticsRunnable implements Runnable {

    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    public void run() {
      while (true) {
        RuntimeStatisticsAggregator runtimeStatisticsAggregator = new RuntimeStatisticsAggregator();
        // Compute aggregate partition runtime statistics
        for (PartitionAccessor partitionAccessor : partitionAccessors) {
          // Check for cancelled status
          if (cancelled.get()) {
            cleanup();
            return;
          }
          if (partitionAccessor != null) {
            runtimeStatisticsAggregator.add(partitionAccessor.getRuntimeStatistics());
          }
        }
        // Set statistics
        try {
          setRuntimeStatistics(hostDomain, runtimeStatisticsAggregator);
        } catch (IOException e) {
          LOG.error("Failed to set runtime statistics", e);
        }
        // Sleep a given interval. Interrupt the thread to stop it while it is sleeping
        try {
          Thread.sleep(updateStatisticsThreadSleepTimeMS);
        } catch (InterruptedException e) {
          cleanup();
          return;
        }
      }
    }

    private void cleanup() {
      try {
        deleteRuntimeStatistics();
      } catch (IOException e) {
        LOG.error("Error while deleting runtime statistics.", e);
        throw new RuntimeException(e);
      }
    }

    public void cancel() {
      cancelled.set(true);
    }
  }

  public String getName() {
    return hostDomain.getDomain().getName();
  }

  public void shutDown() {
    updateStatisticsRunnable.cancel();
    updateStatisticsThread.interrupt();
    try {
      updateStatisticsThread.join();
    } catch (InterruptedException e) {
      LOG.info("Interrupted while waiting for update statistics thread to terminate during shutdown.");
    }
  }

  public static RuntimeStatisticsAggregator getRuntimeStatistics(HostDomain hostDomain) throws IOException {
    String runtimeStatistics = hostDomain.getStatistic(RUNTIME_STATISTICS_KEY);
    if (runtimeStatistics == null) {
      return new RuntimeStatisticsAggregator();
    } else {
      return new RuntimeStatisticsAggregator(runtimeStatistics);
    }
  }

  public static void setRuntimeStatistics(HostDomain hostDomain,
                                          RuntimeStatisticsAggregator runtimeStatisticsAggregator) throws IOException {
    hostDomain.setEphemeralStatistic(RUNTIME_STATISTICS_KEY,
        RuntimeStatisticsAggregator.toString(runtimeStatisticsAggregator));
  }

  public void deleteRuntimeStatistics() throws IOException {
    hostDomain.deleteStatistic(RUNTIME_STATISTICS_KEY);
  }
}
