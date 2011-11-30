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

import com.rapleaf.hank.coordinator.HostDomainPartition;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.performance.HankTimer;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.ReaderResult;
import com.rapleaf.hank.util.AtomicLongCollection;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Wrapper class that stores: 1. HostDomainPartition 2. Reader: The Reader
 * associated with the HostDomainPartition 3. AtomicLongCollection: counters
 * for number of requests and hits in a given time window
 */
public class PartitionAccessor {
  private static final HankResponse NOT_FOUND = HankResponse.not_found(true);
  private static final Logger LOG = Logger.getLogger(PartitionAccessor.class);
  public static final String RUNTIME_STATISTICS_KEY = "runtime_statistics";

  private final HostDomainPartition partition;
  private final Reader reader;
  private final HankTimer windowTimer = new HankTimer();
  private final AtomicLongCollection numRequestsAndHitsInWindow;

  public PartitionAccessor(HostDomainPartition partition, Reader reader) {
    if (reader == null) {
      throw new IllegalArgumentException("Reader may not be null!");
    }
    this.partition = partition;
    this.reader = reader;
    windowTimer.restart();
    numRequestsAndHitsInWindow = new AtomicLongCollection(2);
    numRequestsAndHitsInWindow.set(0, 0);
  }

  public HostDomainPartition getHostDomainPartition() {
    return partition;
  }

  public HankResponse get(ByteBuffer key, ReaderResult result) throws IOException {
    // Increment requests counter
    LOG.trace("Partition GET");
    reader.get(key, result);
    if (result.isFound()) {
      // Increment both num requests and num hits
      numRequestsAndHitsInWindow.increment(1, 1);
      return HankResponse.value(result.getBuffer());
    } else {
      // Increment only num requests
      numRequestsAndHitsInWindow.increment(1, 0);
      return NOT_FOUND;
    }
  }

  public void updateRuntimeStatistics() throws IOException {
    // Copy duration and counts
    long windowDurationNanos = windowTimer.getDuration();
    windowTimer.restart();
    // Get atomic counters
    String countersString = numRequestsAndHitsInWindow.getAsStringAndSet(0, 0);
    // Update statistics
    partition.setEphemeralStatistic(RUNTIME_STATISTICS_KEY, windowDurationNanos + " " + countersString);
  }

  public void deleteRuntimeStatistics() throws IOException {
    partition.deleteStatistic(RUNTIME_STATISTICS_KEY);
  }
}
