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

package com.liveramp.hank.coordinator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ServingStatusAggregator {

  private Map<Domain, Map<Integer, ServingStatus>> domainToPartitionToPartitionServingStatus =
      new HashMap<Domain, Map<Integer, ServingStatus>>();

  public ServingStatusAggregator() {
  }

  public void add(Domain domain, int partitionNumber, boolean servedAndUpToDate) {
    ServingStatus partitionServingStatus = getPartitionServingStatus(domain, partitionNumber);
    // Compute counts
    partitionServingStatus.numPartitions += 1;
    if (servedAndUpToDate) {
      partitionServingStatus.numPartitionsServedAndUpToDate += 1;
    }
  }

  public ServingStatus getPartitionServingStatus(Domain domain, int partitionNumber) {
    Map<Integer, ServingStatus> partitionToPartitionServingStatus =
        domainToPartitionToPartitionServingStatus.get(domain);
    if (partitionToPartitionServingStatus == null) {
      partitionToPartitionServingStatus = new HashMap<Integer, ServingStatus>();
      domainToPartitionToPartitionServingStatus.put(domain, partitionToPartitionServingStatus);
    }
    ServingStatus partitionServingStatus =
        partitionToPartitionServingStatus.get(partitionNumber);
    if (partitionServingStatus == null) {
      partitionServingStatus = new ServingStatus();
      partitionToPartitionServingStatus.put(partitionNumber, partitionServingStatus);
    }
    return partitionServingStatus;
  }

  public void aggregate(ServingStatusAggregator other) {
    for (Map.Entry<Domain, Map<Integer, ServingStatus>> entry1 :
        other.domainToPartitionToPartitionServingStatus.entrySet()) {
      Domain domain = entry1.getKey();
      for (Map.Entry<Integer, ServingStatus> entry2 : entry1.getValue().entrySet()) {
        Integer partitionNumber = entry2.getKey();
        ServingStatus partitionServingStatus = entry2.getValue();
        ServingStatus partitionServingStatusAggregate = getPartitionServingStatus(domain, partitionNumber);
        partitionServingStatusAggregate.aggregate(partitionServingStatus);
      }
    }
  }

  // Compute, for each domain, the number of _host domain partitions_ (non unique partitions)
  // that are served and up to date
  public ServingStatus computeServingStatus() {
    ServingStatus result = new ServingStatus();
    for (Map.Entry<Domain, Map<Integer, ServingStatus>> entry1 :
        domainToPartitionToPartitionServingStatus.entrySet()) {
      for (Map.Entry<Integer, ServingStatus> entry2 : entry1.getValue().entrySet()) {
        ServingStatus partitionServingStatus = entry2.getValue();
        result.aggregate(partitionServingStatus);
      }
    }
    return result;
  }

  // Compute, for each domain, the number of _domain partitions_ (unique partitions)
  // that are fully (all copies) served and up to date
  public ServingStatus computeUniquePartitionsServingStatus(DomainGroup domainGroup) throws IOException {
    ServingStatus result = new ServingStatus();
    for (DomainAndVersion dgvdv : domainGroup.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      Map<Integer, ServingStatus> partitionToServingStatus = domainToPartitionToPartitionServingStatus.get(domain);
      int partitionsServedAndUpToDate = 0;
      if (partitionToServingStatus != null) {
        for (ServingStatus servingStatus : partitionToServingStatus.values()) {
          // A unique partition is served and up to date if all host corresponding domain partitions are
          // served and up to date
          if (servingStatus.getNumPartitionsServedAndUpToDate() == servingStatus.getNumPartitions()) {
            partitionsServedAndUpToDate += 1;
          }
        }
      }
      result.aggregate(domain.getNumParts(), partitionToServingStatus == null ? 0 : partitionsServedAndUpToDate);
    }
    return result;
  }
}
