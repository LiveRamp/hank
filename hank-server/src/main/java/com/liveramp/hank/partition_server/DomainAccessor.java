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

import com.liveramp.commons.util.BytesUtils;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.generated.HankException;
import com.liveramp.hank.generated.HankResponse;
import com.liveramp.hank.partitioner.Partitioner;
import com.liveramp.hank.storage.ReaderResult;
import com.liveramp.hank.util.HankTimer;
import com.liveramp.hank.util.HankTimerEventAggregator;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Class that manages accessing data on behalf of a particular Domain.
 */
public class DomainAccessor {

  private static final HankResponse WRONG_HOST = HankResponse.xception(HankException.wrong_host(true));

  private static final Logger LOG = LoggerFactory.getLogger(DomainAccessor.class);
  private final HostDomain hostDomain;
  private final PartitionAccessor[] partitionAccessors;
  private final Partitioner partitioner;
  private final HankTimerEventAggregator getRequestsTimerAggregator;

  DomainAccessor(HostDomain hostDomain,
                 PartitionAccessor[] partitionAccessors,
                 Partitioner partitioner,
                 int getTimerAggregatorWindow) throws IOException {
    this.hostDomain = hostDomain;
    this.partitionAccessors = partitionAccessors;
    this.partitioner = partitioner;
    this.getRequestsTimerAggregator = new HankTimerEventAggregator("GET " + hostDomain.getDomain().getName(),
        getTimerAggregatorWindow);
  }

  public HankResponse get(ByteBuffer key, ReaderResult result) throws IOException {
    HankTimer timer = getRequestsTimerAggregator.getTimer();
    try {
      int partition = partitioner.partition(key, partitionAccessors.length);
      PartitionAccessor partitionAccessor = partitionAccessors[partition];
      if (partitionAccessor == null) {
        LOG.error("Failed to perform get because of an Exception: wrong host for domain: " + hostDomain.getDomain().getName()
            + ", partition: " + partition + ", key: " + BytesUtils.bytesToHexString(key) + ", response: " + WRONG_HOST);
        return WRONG_HOST;
      }
      return partitionAccessor.get(key, result);
    } finally {
      getRequestsTimerAggregator.add(timer);
    }
  }

  public String getName() {
    return hostDomain.getDomain().getName();
  }

  public HostDomain getHostDomain() {
    return hostDomain;
  }

  public void shutDown() {
    // Shutdown partition accessors
    for (PartitionAccessor partitionAccessor : partitionAccessors) {
      if (partitionAccessor != null) {
        partitionAccessor.shutDown();
      }
    }
  }

  public RuntimeStatisticsAggregator getRuntimeStatistics() {
    RuntimeStatisticsAggregator runtimeStatisticsAggregator = new RuntimeStatisticsAggregator();
    for (PartitionAccessor partitionAccessor : partitionAccessors) {
      if (partitionAccessor != null) {
        runtimeStatisticsAggregator.add(partitionAccessor.getRuntimeStatistics());
      }
    }
    runtimeStatisticsAggregator.setGetRequestsPopulationStatistics(
        getRequestsTimerAggregator.getAndResetPopulationStatistics());
    return runtimeStatisticsAggregator;
  }
}
