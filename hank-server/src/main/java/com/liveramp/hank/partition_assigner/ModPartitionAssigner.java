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

package com.liveramp.hank.partition_assigner;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;

import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.Host;

public class ModPartitionAssigner extends AbstractMappingPartitionAssigner implements PartitionAssigner {

  @Override
  protected Map<Integer, Host> getPartitionsAssignment(Domain domain, SortedSet<Host> validHosts) {
    Map<Integer, Host> result = new HashMap<Integer, Host>();
    for (int partitionNumber = 0; partitionNumber < domain.getNumParts(); ++partitionNumber) {
      // Find a host for this partition
      result.put(partitionNumber, getHostResponsibleForPartition(validHosts, partitionNumber));
    }
    return result;
  }

  protected Host getHostResponsibleForPartition(SortedSet<Host> validHostsSorted, int partitionNumber) {
    // If there are no hosts, simply return null
    if (validHostsSorted.size() == 0) {
      return null;
    }
    int hostIndex = partitionNumber % validHostsSorted.size();
    for (Host host : validHostsSorted) {
      if (hostIndex-- == 0) {
        return host;
      }
    }
    throw new RuntimeException("This should never get executed. A host should have been found.");
  }
}
