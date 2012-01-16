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

package com.rapleaf.hank.partition_assigner;

import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.Ring;

import java.util.SortedSet;

public class ModPartitionAssigner extends AbstractMappingPartitionAssigner implements PartitionAssigner {

  @Override
  protected Host getHostResponsibleForPartition(Ring ring, int partitionNumber) {
    SortedSet<Host> hostsSorted = ring.getHostsSorted();
    // If there are no hosts, simply return null
    if (hostsSorted.size() == 0) {
      return null;
    }
    int hostIndex = partitionNumber % hostsSorted.size();
    for (Host host : hostsSorted) {
      if (hostIndex-- == 0) {
        return host;
      }
    }
    throw new RuntimeException("This should never get executed. A host should have been found.");
  }
}
