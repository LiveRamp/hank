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
import java.nio.ByteBuffer;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.liveramp.hank.coordinator.HostDomainPartition;
import com.liveramp.hank.generated.HankResponse;
import com.liveramp.hank.storage.Reader;
import com.liveramp.hank.storage.ReaderResult;
import com.liveramp.hank.util.AtomicLongCollection;
import com.liveramp.hank.util.HankTimer;

/**
 * Wrapper class that stores: 1. HostDomainPartition 2. Reader: The Reader
 * associated with the HostDomainPartition 3. AtomicLongCollection: counters
 * for number of requests and hits in a given time window
 */
public class PartitionAccessor {

  private static final HankResponse NOT_FOUND = HankResponse.not_found(true);
  private static final Logger LOG = LoggerFactory.getLogger(PartitionAccessor.class);

  private final HostDomainPartition partition;
  private final Reader reader;
  private final HankTimer windowTimer = new HankTimer();
  //0: num requests
  //1: num hits
  //2: responses data num bytes
  //3: num L1 cache hits
  //4: num L2 cache hits
  private final AtomicLongCollection countersWindow;

  public PartitionAccessor(HostDomainPartition partition, Reader reader) {
    if (reader == null) {
      throw new IllegalArgumentException("Reader may not be null!");
    }
    this.partition = partition;
    this.reader = reader;
    windowTimer.restart();
    countersWindow = new AtomicLongCollection(5, new long[]{0, 0, 0, 0, 0});
  }

  public HostDomainPartition getHostDomainPartition() {
    return partition;
  }

  public HankResponse get(ByteBuffer key, ReaderResult result) throws IOException {
    // Increment requests counter
    LOG.trace("Partition GET");
    reader.get(key, result);
    int l1CacheHit = result.getL1CacheHit() ? 1 : 0;
    int l2CacheHit = result.getL2CacheHit() ? 1 : 0;
    if (result.isFound()) {
      countersWindow.increment(1, 1, result.getBuffer().remaining(), l1CacheHit, l2CacheHit);
      return HankResponse.value(result.getBuffer());
    } else {
      countersWindow.increment(1, 0, 0, l1CacheHit, l2CacheHit);
      return NOT_FOUND;
    }
  }

  public PartitionAccessorRuntimeStatistics getRuntimeStatistics() {
    // Copy duration and counts
    long windowDurationNanos = windowTimer.getDuration();
    windowTimer.restart();
    // Get atomic counters
    long[] counters = countersWindow.getAsArrayAndSet(0, 0, 0, 0, 0);
    long numRequestsInWindow = counters[0];
    long numHitsInWindow = counters[1];
    long responsesNumBytesInWindow = counters[2];
    long numL1CacheHitsInWindow = counters[3];
    long numL2CacheHitsInWindow = counters[4];
    double throughput = 0;
    double responseDataThroughput = 0;
    if (windowDurationNanos != 0) {
      throughput = numRequestsInWindow / (windowDurationNanos / 1000000000d);
      responseDataThroughput = responsesNumBytesInWindow / (windowDurationNanos / 1000000000d);
    }

    return new PartitionAccessorRuntimeStatistics(numRequestsInWindow,
        numHitsInWindow,
        throughput,
        responseDataThroughput,
        numL1CacheHitsInWindow,
        numL2CacheHitsInWindow,
        reader.getCacheStatistics());
  }

  public void shutDown() {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        LOG.error("Exception while closing Reader", e);
      }
    }
  }
}
