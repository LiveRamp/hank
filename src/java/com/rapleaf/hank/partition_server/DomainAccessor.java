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

import com.rapleaf.hank.generated.HankException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.ReaderResult;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Class that manages accessing data on behalf of a particular Domain.
 */
class DomainAccessor {

  private static final HankResponse WRONG_HOST = HankResponse.xception(HankException.wrong_host(true));

  private static final Logger LOG = Logger.getLogger(DomainAccessor.class);
  private final Partitioner partitioner;
  private final String name;
  private final PartitionAccessor[] partitionAccessors;
  private final int updateStatisticsThreadSleepTimeMS;
  private final Thread updateStatisticsThread;

  private static final int UPDATE_STATISTICS_THREAD_SLEEP_TIME_MS_DEFAULT = 30000;

  public DomainAccessor(String name, PartitionAccessor[] partitionAccessors,
                        Partitioner partitioner) throws IOException {
    this(name, partitionAccessors, partitioner, UPDATE_STATISTICS_THREAD_SLEEP_TIME_MS_DEFAULT);
  }

  DomainAccessor(String name, PartitionAccessor[] partitionAccessors,
                 Partitioner partitioner, int updateStatisticsThreadSleepTimeMS) throws IOException {
    this.name = name;
    this.partitionAccessors = partitionAccessors;
    this.partitioner = partitioner;
    this.updateStatisticsThreadSleepTimeMS = updateStatisticsThreadSleepTimeMS;

    updateStatisticsThread = new Thread(new UpdateStatisticsThread(), "Update Statistics");
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
  private class UpdateStatisticsThread implements Runnable {
    public void run() {
      while (true) {
        for (PartitionAccessor partitionAccessor : partitionAccessors) {
          if (partitionAccessor != null) {
            try {
              partitionAccessor.updateRuntimeStatistics();
            } catch (IOException e) {
              LOG.error("Failed to update statistics", e);
            }
          }
        }
        // Interrupt the thread to stop it
        try {
          Thread.sleep(updateStatisticsThreadSleepTimeMS);
        } catch (InterruptedException e) {
          // Delete all runtime statistics
          for (PartitionAccessor partitionAccessor : partitionAccessors) {
            try {
              if (partitionAccessor != null) {
                partitionAccessor.deleteRuntimeStatistics();
              }
            } catch (IOException e1) {
              // Swallow
            }
          }
          break;
        }
      }
    }
  }

  public String getName() {
    return name;
  }

  public void shutDown() {
    updateStatisticsThread.interrupt();
    try {
      updateStatisticsThread.join();
    } catch (InterruptedException e) {
      LOG.info("Interrupted while waiting for update statistics thread to terminate during shutdown.");
    }
  }
}
