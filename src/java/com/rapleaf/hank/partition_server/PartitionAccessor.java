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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wrapper class that stores: 1. HostDomainPartition 2. Reader: The Reader
 * associated with the HostDomainPartition 3. AtomicLong: Requests in last
 * minute counter 4. AtomicLong: Hits in last minute counter
 */
public class PartitionAccessor {
  private static final HankResponse NOT_FOUND = HankResponse.not_found(true);
  private static final Logger LOG = Logger.getLogger(PartitionAccessor.class);
  public static final String RUNTIME_STATISTICS_KEY = "runtime_statistics";

  private final HostDomainPartition partition;
  private final Reader reader;
  private final HankTimer windowTimer = new HankTimer();
  private final AtomicLong numRequestsInWindow = new AtomicLong(0);
  private final AtomicLong numHitsInWindow = new AtomicLong(0);

  public PartitionAccessor(HostDomainPartition partition, Reader reader) {
    if (reader == null) {
      throw new IllegalArgumentException("Reader may not be null!");
    }
    this.partition = partition;
    this.reader = reader;
    windowTimer.restart();
    numRequestsInWindow.set(0);
    numHitsInWindow.set(0);
  }

  public HostDomainPartition getHostDomainPartition() {
    return partition;
  }

  public HankResponse get(ByteBuffer key, ReaderResult result) throws IOException {
    // Increment requests counter
    LOG.trace("Partition GET");
    numRequestsInWindow.incrementAndGet();
    reader.get(key, result);
    if (result.isFound()) {
      // Increment hits counter
      numHitsInWindow.incrementAndGet();
      // TODO: do not copy for single requests or when new size is similar
      return HankResponse.value(result.getBufferDeepCopy());
    } else {
      return NOT_FOUND;
    }
  }

  public void updateRuntimeStatistics() throws IOException {
    // Copy duration and counts
    long windowDurationNanos = windowTimer.getDuration();
    windowTimer.restart();
    // Update atomic counters so that we don't miss any request
    long numRequests = numRequestsInWindow.get();
    numRequestsInWindow.addAndGet(-numRequests);
    long numHits = numHitsInWindow.get();
    numHitsInWindow.addAndGet(-numHits);
    // Update statistics
    partition.setEphemeralStatistic(RUNTIME_STATISTICS_KEY,
        PartitionAccessorRuntimeStatistics.toString(windowDurationNanos, numRequests, numHits));
  }

  public void deleteRuntimeStatistics() throws IOException {
    partition.deleteStatistic(RUNTIME_STATISTICS_KEY);
  }
}
